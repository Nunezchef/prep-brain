/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        bone: '#fdfcfb',
        obsidian: '#1a1a1a',
        zinc400: '#a1a1aa',
        hairline: '#efefec',
      },
      boxShadow: {
        atelier: '0 4px 20px -5px rgba(0,0,0,0.03)',
      },
      borderRadius: {
        atelier: '14px',
      },
      transitionTimingFunction: {
        atelier: 'cubic-bezier(0.2, 0, 0.2, 1)',
      },
    },
  },
  plugins: [],
}
