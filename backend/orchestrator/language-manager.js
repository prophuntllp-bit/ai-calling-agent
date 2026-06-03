const fallbackPhrases = {
  hi: "Kya aap do BHK ya teen BHK mein interested hain?",
  mr: "Tumhala 2 BHK pahije ka 3 BHK?",
  en: "Are you interested in a two BHK or three BHK?",
  ta: "Neengal 2 BHK vendum a, 3 BHK vendum a?",
  te: "Meeru 2 BHK kavali, 3 BHK kavali?",
  kn: "Neevu 2 BHK beku, 3 BHK beku?",
  ml: "Ningalk 2 BHK veno, 3 BHK veno?",
  bn: "Apnar ki 2 BHK lagbe na 3 BHK?",
  gu: "Tamne 2 BHK joiye che ke 3 BHK?",
  pa: "Tuhanu 2 BHK chahida hai ke 3 BHK?",
};

const voiceMap = {
  hi: { male: "hi_male_01", female: "hi_female_01" },
  mr: { male: "mr_male_01", female: "mr_female_01" },
  en: { male: "en_male_01", female: "en_female_01" },
  ta: { male: "ta_male_01", female: "ta_female_01" },
  te: { male: "te_male_01", female: "te_female_01" },
  kn: { male: "kn_male_01", female: "kn_female_01" },
  ml: { male: "ml_male_01", female: "ml_female_01" },
  bn: { male: "bn_male_01", female: "bn_female_01" },
  gu: { male: "gu_male_01", female: "gu_female_01" },
  pa: { male: "pa_male_01", female: "pa_female_01" },
};

function baseLanguage(language = "hi") {
  return String(language || "hi").toLowerCase().split("-")[0] || "hi";
}

function hasMeaningfulText(text = "") {
  return String(text || "").trim().length >= 2;
}

class LanguageManager {
  constructor() {
    this.sessions = new Map();
  }

  initialize(callSid, preferredLanguage = "auto") {
    const normalizedPreferred = preferredLanguage === "auto" ? "auto" : baseLanguage(preferredLanguage);
    this.sessions.set(callSid, {
      detectedLanguage: normalizedPreferred === "auto" ? null : normalizedPreferred,
      preferredLanguage: normalizedPreferred,
      languageVotes: {},
      utterances: [],
    });
  }

  recordUtterance(callSid, language, text) {
    const session = this.sessions.get(callSid);
    if (!session) return;
    const normalizedLanguage = language && language !== "auto" ? baseLanguage(language) : null;
    session.utterances.push({ language: normalizedLanguage || language, text });
    if (!normalizedLanguage) {
      return;
    }
    session.languageVotes[normalizedLanguage] = (session.languageVotes[normalizedLanguage] || 0) + 1;

    if (!session.detectedLanguage) {
      session.detectedLanguage = normalizedLanguage;
      return;
    }

    const currentLanguage = baseLanguage(session.detectedLanguage);
    if (normalizedLanguage === currentLanguage) {
      return;
    }

    const currentVotes = session.languageVotes[currentLanguage] || 0;
    const nextVotes = session.languageVotes[normalizedLanguage] || 0;

    // Switch immediately on the first detection of a new language if:
    // 1. The caller explicitly changed language (first utterance in new language)
    // 2. OR the initial language was a preference (not verified by STT yet)
    // 3. OR new language has 2+ votes (confirmed pattern, not noise)
    const initialPreference = currentLanguage === baseLanguage(session.preferredLanguage || "auto");
    // Require 3 confirmed votes before auto-switching between similar scripts (hi↔mr).
    // ElevenLabs STT often misidentifies Hindi as Marathi on single utterances.
    // English is distinctive enough that a single confident detection can switch.
    const isSimilarScript = (currentLanguage === "hi" && normalizedLanguage === "mr") ||
                            (currentLanguage === "mr" && normalizedLanguage === "hi");
    const confirmedSwitch = isSimilarScript ? nextVotes >= 3 : nextVotes >= 2;
    const firstDetectedSwitch = !isSimilarScript && hasMeaningfulText(text) && nextVotes === 1 && (initialPreference || !session.languageConfirmed);
    const shouldSwitch = firstDetectedSwitch || confirmedSwitch;

    if (shouldSwitch) {
      const prevLang = session.detectedLanguage;
      session.detectedLanguage = normalizedLanguage;
      session.languageConfirmed = true;
      if (prevLang !== normalizedLanguage) {
        console.log(`[lang-switch] ${prevLang} → ${normalizedLanguage} (votes: ${nextVotes})`);
      }
    }
  }

  getLanguage(callSid) {
    const session = this.sessions.get(callSid);
    return session?.detectedLanguage || session?.preferredLanguage || "hi";
  }

  getBaseLanguage(callSid) {
    return baseLanguage(this.getLanguage(callSid));
  }

  isLocked(callSid) {
    const session = this.sessions.get(callSid);
    return Boolean(session?.detectedLanguage);
  }

  fallback(callSid) {
    const language = this.getBaseLanguage(callSid);
    return fallbackPhrases[language] || fallbackPhrases.hi;
  }

  resolveVoice(callSid, gender = "female") {
    const language = this.getBaseLanguage(callSid);
    const voices = voiceMap[language] || voiceMap.hi;
    return voices[gender] || voices.female;
  }

  detectCodeSwitch(callSid, text) {
    const session = this.sessions.get(callSid);
    if (!session) return false;
    const hasLatin = /[a-z]/i.test(text);
    const hasIndic = /[\u0900-\u097F\u0B80-\u0BFF\u0C00-\u0C7F\u0C80-\u0CFF\u0D00-\u0D7F\u0980-\u09FF\u0A80-\u0AFF\u0A00-\u0A7F]/.test(text);
    return hasLatin && hasIndic;
  }

  clear(callSid) {
    this.sessions.delete(callSid);
  }
}

module.exports = { LanguageManager };
