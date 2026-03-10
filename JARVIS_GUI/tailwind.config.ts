import type {Config} from 'tailwindcss';
import tailwindcssAnimate from 'tailwindcss-animate';

export default {
  darkMode: ['class'],
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        body: ['Orbitron', 'sans-serif'],
        headline: ['Orbitron', 'sans-serif'],
        code: ['monospace'],
      },
      colors: {
        background: 'hsl(var(--background))',
        foreground: 'hsl(var(--foreground))',
        card: {
          DEFAULT: 'hsl(var(--card))',
          foreground: 'hsl(var(--card-foreground))',
        },
        popover: {
          DEFAULT: 'hsl(var(--popover))',
          foreground: 'hsl(var(--popover-foreground))',
        },
        primary: {
          DEFAULT: 'hsl(var(--primary))',
          foreground: 'hsl(var(--primary-foreground))',
        },
        secondary: {
          DEFAULT: 'hsl(var(--secondary))',
          foreground: 'hsl(var(--secondary-foreground))',
        },
        muted: {
          DEFAULT: 'hsl(var(--muted))',
          foreground: 'hsl(var(--muted-foreground))',
        },
        accent: {
          DEFAULT: 'hsl(var(--accent))',
          foreground: 'hsl(var(--accent-foreground))',
        },
        destructive: {
          DEFAULT: 'hsl(var(--destructive))',
          foreground: 'hsl(var(--destructive-foreground))',
        },
        border: 'hsl(var(--border))',
        input: 'hsl(var(--input))',
        ring: 'hsl(var(--ring))',
        chart: {
          '1': 'hsl(var(--chart-1))',
          '2': 'hsl(var(--chart-2))',
          '3': 'hsl(var(--chart-3))',
          '4': 'hsl(var(--chart-4))',
          '5': 'hsl(var(--chart-5))',
        },
        sidebar: {
          DEFAULT: 'hsl(var(--sidebar-background))',
          foreground: 'hsl(var(--sidebar-foreground))',
          primary: 'hsl(var(--sidebar-primary))',
          'primary-foreground': 'hsl(var(--sidebar-primary-foreground))',
          accent: 'hsl(var(--sidebar-accent))',
          'accent-foreground': 'hsl(var(--sidebar-accent-foreground))',
          border: 'hsl(var(--sidebar-border))',
          ring: 'hsl(var(--sidebar-ring))',
        },
      },
      borderRadius: {
        lg: 'var(--radius)',
        md: 'calc(var(--radius) - 2px)',
        sm: 'calc(var(--radius) - 4px)',
      },
      keyframes: {
        'accordion-down': {
          from: {
            height: '0',
          },
          to: {
            height: 'var(--radix-accordion-content-height)',
          },
        },
        'accordion-up': {
          from: {
            height: 'var(--radix-accordion-content-height)',
          },
          to: {
            height: '0',
          },
        },
        'fade-in': {
          from: { opacity: '0', transform: 'scale(0.98)' },
          to: { opacity: '1', transform: 'scale(1)' },
        },
        'spin-slow': {
          from: { transform: 'rotate(0deg)' },
          to: { transform: 'rotate(360deg)' },
        },
        'trace-in': {
          to: { strokeDashoffset: '0' },
        },
        'particle-line-in': {
          '0%': {
            transform: 'translate(calc(100px + cos(var(--angle)) * 250px), calc(100px + sin(var(--angle)) * 250px)) rotate(var(--angle))',
            opacity: '0',
          },
          '20%, 80%': {
            opacity: '1',
          },
          '100%': {
            transform: 'translate(calc(100px + cos(var(--angle)) * var(--radius)), calc(100px + sin(var(--angle)) * var(--radius))) rotate(var(--angle))',
            opacity: '0',
          },
        },
        'jarvis-letter-in': {
          '0%': {
            opacity: '0',
            transform: 'translateY(20px) scale(0.8)',
            filter: 'blur(5px)',
          },
          '40%': {
            opacity: '1',
            filter: 'blur(0)',
            textShadow: '0 0 25px hsl(var(--primary)), 0 0 10px hsl(var(--accent))'
          },
          '100%': {
            opacity: '1',
            transform: 'translateY(0) scale(1)',
            textShadow: 'none'
          }
        },
        'scanline': {
          '0%': { transform: 'translateY(-100%)' },
          '100%': { transform: 'translateY(200%)' },
        },
        'pulse-slow': {
          '0%, 100%': { opacity: '0.5', transform: 'scale(1)' },
          '50%': { opacity: '1', transform: 'scale(1.02)' },
        },
        'fade-in-out': {
          '0%': { opacity: '0' },
          '20%': { opacity: '1' },
          '80%': { opacity: '1' },
          '100%': { opacity: '0' },
        },
        'flicker': {
          '0%, 100%': { opacity: '1', 'border-color': 'hsl(var(--accent) / 0.8)' },
          '50%': { opacity: '0.3', 'border-color': 'hsl(var(--accent) / 0.3)' },
        },
        'holographic-flicker': {
          '0%, 100%': { opacity: '1', 'border-color': 'hsl(var(--primary))' },
          '50%': { opacity: '0.6', 'border-color': 'hsl(var(--primary) / 0.5)' },
        },
        'orbit': {
          from: { transform: 'rotate(0deg)' },
          to: { transform: 'rotate(360deg)' },
        },
        'breathing-glow': {
          '0%, 100%': {
            boxShadow: '0 0 50px 12px hsl(var(--primary) / 0.3), inset 0 0 15px hsl(var(--primary) / 0.4)',
            opacity: '0.9',
          },
          '50%': {
            boxShadow: '0 0 70px 18px hsl(var(--primary) / 0.5), inset 0 0 20px hsl(var(--primary) / 0.6)',
            opacity: '1',
          },
        },
        'scanner-line-rotate': {
          from: { transform: 'rotate(0deg)' },
          to: { transform: 'rotate(360deg)' },
        },
        'scanner-line-fade': {
          '0%, 100%': { opacity: '0' },
          '20%, 80%': { opacity: '1' },
        },
        'glyph-float': {
          '0%, 100%': { transform: 'translateY(-20px) rotate(-5deg)', opacity: '0.1' },
          '50%': { transform: 'translateY(20px) rotate(5deg)', opacity: '0.3' },
        },
        'speaking-ripple': {
          'from': { transform: 'scale(0.7)', opacity: '0.8' },
          'to': { transform: 'scale(1.6)', opacity: '0' },
        },
        'energy-pulse': {
          '0%': { transform: 'scale(0.9)', opacity: '0' },
          '50%': { opacity: '0.3' },
          '100%': { transform: 'scale(1.4)', opacity: '0' },
        },
        'waveform-bar': {
          '0%, 100%': { height: '3px', opacity: '0.4' },
          '50%': { height: '15px', opacity: '1' },
        },
        'particle-drift': {
          from: { transform: 'translate(var(--x-start), var(--y-start))', opacity: '0' },
          '20%, 80%': { opacity: '1' },
          to: { transform: 'translate(var(--x-end), var(--y-end))', opacity: '0' },
        },
        'particle-suck-in': {
            'from': {
                transform: 'translate(var(--x-start), var(--y-start)) scale(1.2)',
                opacity: '1'
            },
            'to': {
                transform: 'translate(0, 0) scale(0)',
                opacity: '0'
            }
        },
        'particle-swirl': {
          from: { transform: 'rotate(0deg) scale(1)' },
          to: { transform: 'rotate(-360deg) scale(1.1)' }
        },
        'circuit-pulse': {
          '0%, 100%': { 'background-color': 'hsl(var(--primary)/0.05)' },
          '50%': { 'background-color': 'hsl(var(--primary)/0.15)' }
        },
        'boost-flash': {
          '0%': { opacity: '0' },
          '25%': { opacity: '1' },
          '100%': { opacity: '0' },
        },
        'vignette-pulse': {
            '0%, 100%': { opacity: '0' },
            '50%': { opacity: '0.7' },
        },
      },
      animation: {
        'accordion-down': 'accordion-down 0.2s ease-out',
        'accordion-up': 'accordion-up 0.2s ease-out',
        'fade-in': 'fade-in 1s cubic-bezier(0.215, 0.610, 0.355, 1.000) forwards',
        'spin-slow': 'spin-slow 25s linear infinite',
        'trace-in': 'trace-in 2s cubic-bezier(0.215, 0.610, 0.355, 1.000) 1s forwards',
        'particle-line-in': 'particle-line-in var(--duration) cubic-bezier(0.4, 0, 0.2, 1) var(--delay) forwards',
        'jarvis-letter-in': 'jarvis-letter-in 1.2s cubic-bezier(0.215, 0.610, 0.355, 1.000) forwards',
        'scanline': 'scanline 1s linear 3s',
        'pulse-slow': 'pulse-slow 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'fade-in-out': 'fade-in-out forwards',
        'flicker': 'flicker 2.5s linear infinite',
        'holographic-flicker': 'holographic-flicker 3s linear infinite',
        'orbit': 'orbit var(--duration, 20s) linear infinite var(--delay, 0s)',
        'breathing-glow': 'breathing-glow 4s ease-in-out infinite',
        'scanner-line-rotate': 'scanner-line-rotate var(--speed, 8s) linear infinite',
        'scanner-line-fade': 'scanner-line-fade calc(var(--speed, 8s) / 2) ease-in-out infinite',
        'glyph-float': 'glyph-float var(--duration, 5s) ease-in-out infinite alternate var(--delay, 0s)',
        'speaking-ripple': 'speaking-ripple 1.5s cubic-bezier(0.22, 1, 0.36, 1) forwards',
        'energy-pulse': 'energy-pulse 1.5s ease-out infinite',
        'waveform-bar': 'waveform-bar 0.8s ease-in-out infinite alternate',
        'particle-drift': 'particle-drift var(--duration, 20s) linear infinite alternate var(--delay, 0s)',
        'particle-suck-in': 'particle-suck-in forwards',
        'particle-swirl': 'particle-swirl 6s ease-in-out forwards',
        'circuit-pulse': 'circuit-pulse 5s ease-in-out infinite',
        'boost-flash': 'boost-flash 0.5s ease-out forwards',
        'vignette-pulse': 'vignette-pulse 1.5s cubic-bezier(0.4, 0, 0.6, 1) forwards',
      },
    },
  },
  plugins: [tailwindcssAnimate],
} satisfies Config;
