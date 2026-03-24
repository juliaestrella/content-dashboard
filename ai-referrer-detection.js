/**
 * AI Referrer Detection
 * Runs before any other analytics calls on every page load.
 * Detects AI-sourced traffic via document.referrer and UTM parameters.
 * Stores result as session property `ai_referral_source`.
 */
(function detectAIReferrer() {
  var AI_DOMAINS = {
    'chatgpt.com':       'ChatGPT',
    'chat.openai.com':   'ChatGPT',
    'perplexity.ai':     'Perplexity',
    'claude.ai':         'Claude',
    'gemini.google.com': 'Gemini',
    'bard.google.com':   'Gemini',
    'you.com':           'You.com'
  };

  var source = null;

  // 1. Check document.referrer
  if (document.referrer) {
    try {
      var refHost = new URL(document.referrer).hostname.replace(/^www\./, '');
      // Exact match first
      if (AI_DOMAINS[refHost]) {
        source = AI_DOMAINS[refHost];
      } else {
        // Subdomain match (e.g. app.perplexity.ai)
        var keys = Object.keys(AI_DOMAINS);
        for (var i = 0; i < keys.length; i++) {
          if (refHost === keys[i] || refHost.endsWith('.' + keys[i])) {
            source = AI_DOMAINS[keys[i]];
            break;
          }
        }
      }
    } catch (e) {
      // Malformed referrer — ignore
    }
  }

  // 2. Check UTM parameters (override referrer if present)
  if (!source) {
    try {
      var params = new URLSearchParams(window.location.search);
      var utmMedium = (params.get('utm_medium') || '').toLowerCase();
      var utmSource = (params.get('utm_source') || '').toLowerCase();

      // utm_medium=ai_referral → flag as AI but check utm_source for specifics
      if (utmMedium === 'ai_referral' || utmMedium === 'ai') {
        source = 'AI (unknown)';
      }

      // utm_source matching a known AI domain or label
      if (utmSource) {
        var utmMap = {
          'chatgpt':         'ChatGPT',
          'chatgpt.com':     'ChatGPT',
          'chat.openai.com': 'ChatGPT',
          'openai':          'ChatGPT',
          'perplexity':      'Perplexity',
          'perplexity.ai':   'Perplexity',
          'claude':          'Claude',
          'claude.ai':       'Claude',
          'anthropic':       'Claude',
          'gemini':          'Gemini',
          'gemini.google.com': 'Gemini',
          'bard':            'Gemini',
          'bard.google.com': 'Gemini',
          'you.com':         'You.com',
          'you':             'You.com'
        };
        if (utmMap[utmSource]) {
          source = utmMap[utmSource];
        }
      }
    } catch (e) {
      // URLSearchParams not supported or error — ignore
    }
  }

  // 3. Store as session property
  // Works with Amplitude, Segment, or any analytics layer.
  // The property persists for the session via sessionStorage.
  var STORAGE_KEY = 'ai_referral_source';

  if (source) {
    sessionStorage.setItem(STORAGE_KEY, source);
  } else {
    // Preserve value from earlier page in same session
    source = sessionStorage.getItem(STORAGE_KEY) || null;
  }

  // Expose globally for analytics calls
  window.ai_referral_source = source;

  // --- Amplitude integration ---
  if (typeof amplitude !== 'undefined') {
    // User property: first-touch only (setOnce never overwrites)
    var identify = new amplitude.Identify();
    identify.setOnce('ai_referral_source', source);
    amplitude.identify(identify);

    // Session property: set on every session so it reflects the current visit
    amplitude.setGroup('ai_referral_source', source || 'none');

    // Dedicated event: only fire when an AI referrer is detected
    if (source) {
      // Determine page type from URL path
      var path = window.location.pathname;
      var pageType = null;
      if (path.indexOf('/blog/') === 0 || path.indexOf('/blog/') > 0) {
        pageType = 'blog';
      } else if (path.indexOf('/automations/') === 0 || path.indexOf('/automations/') > 0) {
        pageType = 'phantom_page';
      }

      // Extract keyword from page metadata if available
      var keyword = null;
      var metaKeywords = document.querySelector('meta[name="keywords"]');
      if (metaKeywords) {
        keyword = metaKeywords.getAttribute('content');
      }
      if (!keyword) {
        var metaDesc = document.querySelector('meta[name="description"]');
        if (metaDesc) {
          keyword = metaDesc.getAttribute('content');
        }
      }
      // Also check for a data attribute that the CMS may set
      var keywordEl = document.querySelector('[data-primary-keyword]');
      if (keywordEl) {
        keyword = keywordEl.getAttribute('data-primary-keyword');
      }

      // Fire once per session — guard with sessionStorage flag
      var eventFiredKey = 'ai_referral_event_fired';
      if (!sessionStorage.getItem(eventFiredKey)) {
        amplitude.track('AI Referral Session Started', {
          source: source,
          landing_page: path,
          page_type: pageType,
          keyword: keyword,
          referrer_url: document.referrer || null
        });
        sessionStorage.setItem(eventFiredKey, '1');
      }
    }
  }

  // --- Google Analytics 4 (gtag) integration ---
  if (typeof gtag === 'function') {
    gtag('set', 'user_properties', {
      ai_referral_source: source || '(none)'
    });
    if (source) {
      gtag('event', 'ai_referral_detected', {
        ai_source: source,
        landing_page: window.location.pathname
      });
    }
  }

  // --- dataLayer push (GTM) ---
  window.dataLayer = window.dataLayer || [];
  window.dataLayer.push({
    event: 'ai_referral_detected',
    ai_referral_source: source
  });
})();
