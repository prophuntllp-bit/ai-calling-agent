/**
 * agni-bridge.js
 *
 * Bridges EnableX telephony audio ↔ Ravan.ai Agni AI agent via LiveKit.
 *
 * Audio contract:
 *   - Caller audio IN  : PCM16 mono 16 kHz  (already decoded by decodeEnablexInboundMedia)
 *   - Agent audio OUT  : PCM16 mono 16 kHz  (handed back to sendEnablexMuLaw for μ-law encoding)
 *
 * Everything else (VAD, STT, LLM, TTS, barge-in) is handled inside Agni's cloud.
 */

"use strict";

const AGNI_API_BASE = "https://api.ravan.ai/api/v1";
const SAMPLE_RATE = 16000;
const CHANNELS = 1;

// ---------------------------------------------------------------------------
// Lazy-load the LiveKit SDK so the module doesn't crash when the package is
// missing (old Railway builds that haven't been rebuilt yet).
// ---------------------------------------------------------------------------
let livekitSdk = null;
function getLiveKit() {
  if (!livekitSdk) {
    try {
      livekitSdk = require("@livekit/rtc-node");
    } catch (err) {
      throw new Error(
        "@livekit/rtc-node not installed. Run: npm install @livekit/rtc-node"
      );
    }
  }
  return livekitSdk;
}

// ---------------------------------------------------------------------------
// createAgniSession
// ---------------------------------------------------------------------------
/**
 * Call the Agni REST API to create a new "web_call" session.
 * Returns { url, access_token, session_id } from the Agni API.
 */
async function createAgniSession({ apiKey, agentId, callSid, dynamicVariables = {} }) {
  const body = {
    type: "web_call",
    agent_id: agentId,
    metadata: { callSid },
    prompt_dynamic_variables: dynamicVariables,
  };

  const res = await fetch(`${AGNI_API_BASE}/calling/create-call`, {
    method: "POST",
    headers: {
      "X-Api-Key": apiKey,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Agni API ${res.status}: ${text}`);
  }

  const json = await res.json();
  if (!json?.data?.url || !json?.data?.access_token) {
    throw new Error(`Agni API returned unexpected shape: ${JSON.stringify(json)}`);
  }

  return json.data; // { url, access_token, session_id }
}

// ---------------------------------------------------------------------------
// AgniBridge
// ---------------------------------------------------------------------------
class AgniBridge {
  /**
   * @param {object} opts
   * @param {string} opts.callSid
   * @param {string} opts.livekitUrl   - from Agni create-call response
   * @param {string} opts.token        - LiveKit access_token
   * @param {function(Buffer):void} opts.onAgentAudio   - called with PCM16@16kHz chunks
   * @param {function(string):void}  opts.onDisconnect  - called when LiveKit room closes
   */
  constructor({ callSid, livekitUrl, token, onAgentAudio, onDisconnect }) {
    this.callSid = callSid;
    this.livekitUrl = livekitUrl;
    this.token = token;
    this.onAgentAudio = onAgentAudio;
    this.onDisconnect = onDisconnect;

    this.room = null;
    this.audioSource = null;
    this.connected = false;
    this._disconnecting = false;
  }

  // --------------------------------------------------------------------------
  // connect() — establish LiveKit room, publish caller track, subscribe output
  // --------------------------------------------------------------------------
  async connect() {
    const lk = getLiveKit();
    const {
      Room,
      RoomEvent,
      AudioStream,
      AudioSource,
      LocalAudioTrack,
      AudioFrame,
      TrackKind,
    } = lk;

    this.room = new Room();

    // --- Agent audio output (Agni → EnableX) --------------------------------
    this.room.on(RoomEvent.TrackSubscribed, (track, _pub, participant) => {
      if (track.kind !== TrackKind.KIND_AUDIO) return;
      console.log(
        `[agni-bridge] subscribed audio from ${participant.identity} callSid=${this.callSid}`
      );

      // AudioStream auto-resamples to our requested SAMPLE_RATE
      const stream = new AudioStream(track, SAMPLE_RATE, CHANNELS);

      (async () => {
        try {
          for await (const frame of stream) {
            if (!this.connected) break;
            if (!this.onAgentAudio) continue;
            // frame.data is Int16Array — wrap in Buffer without copying
            const pcm = Buffer.from(
              frame.data.buffer,
              frame.data.byteOffset,
              frame.data.byteLength
            );
            this.onAgentAudio(pcm);
          }
        } catch (err) {
          if (this.connected) {
            console.warn(
              `[agni-bridge] audio stream error callSid=${this.callSid}`,
              err.message
            );
          }
        }
      })();
    });

    // --- Disconnect event ----------------------------------------------------
    this.room.on(RoomEvent.Disconnected, (reason) => {
      console.log(
        `[agni-bridge] room disconnected callSid=${this.callSid} reason=${reason || "unknown"}`
      );
      this.connected = false;
      if (!this._disconnecting && this.onDisconnect) {
        this.onDisconnect(String(reason || "remote"));
      }
    });

    // --- Caller audio source (EnableX → Agni) --------------------------------
    this.audioSource = new AudioSource(SAMPLE_RATE, CHANNELS);
    const callerTrack = LocalAudioTrack.createAudioTrack("caller-audio", this.audioSource);

    // Connect then publish
    await this.room.connect(this.livekitUrl, this.token, { autoSubscribe: true });
    await this.room.localParticipant.publishTrack(callerTrack);

    this.connected = true;
    console.log(`[agni-bridge] connected to LiveKit callSid=${this.callSid}`);
  }

  // --------------------------------------------------------------------------
  // pushCallerAudio(pcm16Buffer)
  //   Forward a raw PCM16@16kHz buffer from EnableX into the LiveKit room.
  //   Called for EVERY inbound audio frame — no buffering needed.
  // --------------------------------------------------------------------------
  pushCallerAudio(pcm16Buffer) {
    if (!this.audioSource || !this.connected || !pcm16Buffer?.length) return;

    try {
      const lk = getLiveKit();
      const { AudioFrame } = lk;

      // Ensure we have an even byte count (Int16 = 2 bytes per sample)
      const bytes = pcm16Buffer.length % 2 === 0
        ? pcm16Buffer
        : pcm16Buffer.subarray(0, pcm16Buffer.length - 1);

      const samplesPerChannel = bytes.length / 2;
      const int16 = new Int16Array(
        bytes.buffer,
        bytes.byteOffset,
        samplesPerChannel
      );

      const frame = new AudioFrame(int16, SAMPLE_RATE, CHANNELS, samplesPerChannel);
      this.audioSource.captureFrame(frame);
    } catch (_err) {
      // non-fatal — next frame will try again
    }
  }

  // --------------------------------------------------------------------------
  // disconnect()
  // --------------------------------------------------------------------------
  async disconnect() {
    if (this._disconnecting) return;
    this._disconnecting = true;
    this.connected = false;
    try {
      if (this.room) await this.room.disconnect();
    } catch (_err) {
      // best-effort
    }
    console.log(`[agni-bridge] disconnected callSid=${this.callSid}`);
  }
}

module.exports = { AgniBridge, createAgniSession };
