const fallbackPhrases = {
  hi: "Ek second, main check karta hoon.",
  mr: "Thoda vel dya, mi tapasun sangto.",
  en: "One moment, let me check that for you.",
  ta: "Oru nimisham, naan check panren.",
  te: "Oka nimisham, nenu chusi cheptanu.",
  kn: "Ondu nimisha, nodi helthini.",
  ml: "Oru nimisham, njan nokki parayam.",
  bn: "Ekto opekkha korun, ami dekhe bolchi.",
  gu: "Ek minute, hu check kari ne kahu chu.",
  pa: "Ik minute, main check karke dassda haan.",
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

class LanguageManager {
  constructor() {
    this.sessions = new Map();
  }

  initialize(callSid, preferredLanguage = "auto") {
    this.sessions.set(callSid, {
      detectedLanguage: preferredLanguage === "auto" ? null : preferredLanguage,
      preferredLanguage,
      utterances: [],
    });
  }

  recordUtterance(callSid, language, text) {
    const session = this.sessions.get(callSid);
    if (!session) return;
    session.utterances.push({ language, text });
    if (!session.detectedLanguage && language && language !== "auto") {
      session.detectedLanguage = language;
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
