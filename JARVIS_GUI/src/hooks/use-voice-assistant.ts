'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import { backendClient } from '@/lib/backend-client';

export type AiStatus = 'idle' | 'listening' | 'thinking' | 'speaking' | 'error';

interface SpeechRecognitionAlternativeLike {
  transcript: string;
}

interface SpeechRecognitionResultLike {
  readonly isFinal: boolean;
  readonly length: number;
  [index: number]: SpeechRecognitionAlternativeLike;
}

interface SpeechRecognitionResultListLike {
  readonly length: number;
  [index: number]: SpeechRecognitionResultLike;
}

interface SpeechRecognitionEventLike {
  readonly results?: SpeechRecognitionResultListLike;
}

interface SpeechRecognitionErrorEventLike {
  readonly error?: string;
}

type SpeechRecognitionLike = {
  continuous: boolean;
  interimResults: boolean;
  lang: string;
  start: () => void;
  stop: () => void;
  abort: () => void;
  onresult: ((event: SpeechRecognitionEventLike) => void) | null;
  onend: (() => void) | null;
  onerror: ((event: SpeechRecognitionErrorEventLike) => void) | null;
};

type SpeechRecognitionCtor = new () => SpeechRecognitionLike;

interface WindowWithSpeechRecognition extends Window {
  SpeechRecognition?: SpeechRecognitionCtor;
  webkitSpeechRecognition?: SpeechRecognitionCtor;
}

const getSpeechRecognitionCtor = (): SpeechRecognitionCtor | null => {
  if (typeof window === 'undefined') return null;
  const speechWindow = window as WindowWithSpeechRecognition;
  return speechWindow.SpeechRecognition || speechWindow.webkitSpeechRecognition || null;
};

const getErrorMessage = (value: unknown): string => {
  if (value instanceof Error) return value.message;
  if (typeof value === 'string') return value;
  return 'Unknown backend error.';
};

export const useVoiceAssistant = (onClose: () => void) => {
  const [status, setStatus] = useState<AiStatus>('idle');
  const [transcript, setTranscript] = useState('');
  const [assistantReply, setAssistantReply] = useState('');
  const [error, setError] = useState('');
  const [audioDataUri, setAudioDataUri] = useState<string | null>(null);
  const [ttsProviderHint, setTtsProviderHint] = useState('auto');

  const recognitionRef = useRef<SpeechRecognitionLike | null>(null);
  const wakeTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const startListening = useCallback(() => {
    if (recognitionRef.current && status !== 'listening') {
      try {
        setTranscript('');
        setAssistantReply('');
        setError('');
        setAudioDataUri(null);
        recognitionRef.current.start();
        setStatus('listening');
      } catch (eventError) {
        console.error('Could not start recognition', eventError);
      }
    }
  }, [status]);

  useEffect(() => {
    const SpeechCtor = getSpeechRecognitionCtor();

    if (!SpeechCtor) {
      setError('Speech recognition not supported in this runtime.');
      setStatus('error');
      return;
    }

    if (!recognitionRef.current) {
      const recognition = new SpeechCtor();
      recognition.continuous = false;
      recognition.interimResults = true;
      recognition.lang = 'en-US';

      recognition.onresult = (event: SpeechRecognitionEventLike) => {
        let finalTranscript = '';
        let interimTranscript = '';
        const results = event?.results ?? [];

        for (let i = 0; i < results.length; i += 1) {
          const item = results[i];
          const piece = item?.[0]?.transcript || '';
          if (item?.isFinal) {
            finalTranscript += piece;
          } else {
            interimTranscript += piece;
          }
        }

        if (finalTranscript) {
          setTranscript(finalTranscript);
          recognition.stop();
          setStatus('thinking');
        } else {
          setTranscript(interimTranscript);
        }
      };

      recognition.onend = () => {
        setStatus((prev) => (prev === 'listening' ? 'thinking' : prev));
      };

      recognition.onerror = (event: SpeechRecognitionErrorEventLike) => {
        const code = String(event?.error || 'unknown');
        console.error('Speech recognition error', code);
        if (code === 'no-speech') {
          onClose();
        } else {
          setError(`Speech error: ${code}`);
          setStatus('error');
        }
      };

      recognitionRef.current = recognition;
    }
  }, [onClose]);

  useEffect(() => {
    let cancelled = false;
    const hydrateTtsProvider = async () => {
      try {
        const diagnostics = await backendClient.ttsDiagnostics({ history_limit: 8 });
        if (cancelled) return;
        const recommended = String(diagnostics.recommended_provider || '').trim().toLowerCase();
        if (recommended) {
          setTtsProviderHint(recommended);
        }
      } catch {
        // keep auto mode on diagnostics failures
      }
    };
    void hydrateTtsProvider();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const processTranscript = async () => {
      if (status === 'thinking' && transcript) {
        try {
          const response = await backendClient.chat({
            message: transcript,
            history: [],
          });

          const reply = response.reply || 'Task received.';
          setAssistantReply(reply);

          try {
            await backendClient.speak({
              text: reply,
              provider: ttsProviderHint || 'auto',
              allow_text_fallback: true,
            });
          } catch (ttsError) {
            console.warn('TTS playback request failed:', ttsError);
            try {
              await backendClient.speak(reply);
            } catch (fallbackError) {
              console.warn('Fallback TTS playback request failed:', fallbackError);
            }
          }

          // Desktop backend handles voice playback directly; UI still moves through speaking state.
          setStatus('speaking');
        } catch (interactionError: unknown) {
          console.error('Error with backend interaction', interactionError);
          setError(getErrorMessage(interactionError));
          setStatus('error');
        }
      } else if (status === 'thinking' && !transcript) {
        onClose();
      }
    };
    processTranscript();
  }, [status, transcript, onClose, ttsProviderHint]);

  useEffect(() => {
    wakeTimeoutRef.current = setTimeout(() => {
      startListening();
    }, 1000);

    return () => {
      if (wakeTimeoutRef.current) {
        clearTimeout(wakeTimeoutRef.current);
      }
      if (recognitionRef.current) {
        recognitionRef.current.abort();
      }
    };
  }, [startListening]);

  useEffect(() => {
    if (status === 'error') {
      const timer = setTimeout(() => onClose(), 5000);
      return () => clearTimeout(timer);
    }
  }, [status, onClose]);

  useEffect(() => {
    if (status === 'speaking' && !audioDataUri) {
      const timer = setTimeout(() => onClose(), 2400);
      return () => clearTimeout(timer);
    }
  }, [status, audioDataUri, onClose]);

  return { status, transcript, assistantReply, error, audioDataUri };
};
