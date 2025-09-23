const SystemPrompt = require('../models/SystemPrompt');

/**
 * Get the default system prompt from the database
 * @returns {Promise<string>} The default promptText or fallback policy
 */
const getDefaultSystemPrompt = async () => {
  try {
    // Find the default system prompt
    const defaultPrompt = await SystemPrompt.findOne({ isDefault: true })
      .select('promptText')
      .lean();
    
    if (defaultPrompt && defaultPrompt.promptText) {
      console.log('✅ [SYSTEM-PROMPT] Using default prompt from database');
      return defaultPrompt.promptText;
    }
    
    // Fallback to hardcoded policy if no default found
    console.log('⚠️ [SYSTEM-PROMPT] No default prompt found, using fallback');
    return getFallbackPolicyBlock();
  } catch (error) {
    console.error('❌ [SYSTEM-PROMPT] Error fetching default prompt:', error.message);
    return getFallbackPolicyBlock();
  }
};

/**
 * Get fallback policy block when database is unavailable
 * @returns {string} Hardcoded fallback policy
 */
const getFallbackPolicyBlock = () => {
  return [
    "Answer strictly using the information provided above.",
    "If the user asks for address, phone, timings, or other specifics, check the System Prompt or FirstGreeting.",
    "If the information is not present, reply briefly that you don't have that information.",
    "Always end your answer with a short, relevant follow-up question to keep the conversation going.",
    "Keep the entire reply under 100 tokens.",
    "Use the language of the user's message in the same language give the response.",
  ].join(" ");
};

/**
 * Get system prompt with caching for performance
 * @returns {Promise<string>} The system prompt text
 */
let cachedPrompt = null;
let lastCacheTime = 0;
const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

const getSystemPromptWithCache = async () => {
  const now = Date.now();
  
  // Return cached prompt if still valid
  if (cachedPrompt && (now - lastCacheTime) < CACHE_DURATION) {
    return cachedPrompt;
  }
  
  // Fetch fresh prompt from database
  cachedPrompt = await getDefaultSystemPrompt();
  lastCacheTime = now;
  
  return cachedPrompt;
};

/**
 * Clear the cached prompt (useful for testing or when prompts are updated)
 */
const clearSystemPromptCache = () => {
  cachedPrompt = null;
  lastCacheTime = 0;
  console.log('🔄 [SYSTEM-PROMPT] Cache cleared');
};

module.exports = {
  getDefaultSystemPrompt,
  getFallbackPolicyBlock,
  getSystemPromptWithCache,
  clearSystemPromptCache
};
