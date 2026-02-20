import React from 'react';
import { Heart } from 'lucide-react';

const GITHUB_URL = 'https://github.com/baardie/LoomPipe';

const Footer = () => (
  <footer className="border-t border-[var(--border)] py-3 px-4 text-center text-xs text-[var(--text-muted)]">
    Built by{' '}
    <a href="mailto:lukebaard@outlook.com" className="hover:text-[var(--text-secondary)] transition-colors">
      Luke Baard
    </a>
    {' · '}
    <a href={GITHUB_URL} target="_blank" rel="noopener noreferrer" className="hover:text-[var(--text-secondary)] transition-colors">
      GitHub
    </a>
    {' · '}
    <a
      href="https://www.paypal.com/paypalme/baardie"
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center gap-1 hover:text-[var(--text-secondary)] transition-colors"
    >
      <Heart size={11} className="text-[var(--red)]" />
      Buy me a coffee
    </a>
  </footer>
);

export default Footer;
