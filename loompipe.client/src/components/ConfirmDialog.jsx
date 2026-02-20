import { useEffect } from 'react';
import { AlertTriangle } from 'lucide-react';

/**
 * Reusable destructive-action confirmation modal.
 *
 * Props:
 *   open        – boolean
 *   title       – e.g. "Delete Pipeline"
 *   message     – body text, can include the item name
 *   confirmLabel – button label (default "Delete")
 *   onConfirm   – called when the user clicks the danger button
 *   onCancel    – called on Cancel, backdrop click, or Escape
 */
const ConfirmDialog = ({
  open,
  title,
  message,
  confirmLabel = 'Delete',
  onConfirm,
  onCancel,
}) => {
  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handler = (e) => { if (e.key === 'Escape') onCancel(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open, onCancel]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
      onClick={onCancel}
    >
      <div
        className="bg-slate-900 border border-slate-700 rounded-xl w-full max-w-sm shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="p-6">
          <div className="flex items-start gap-4">
            <div className="p-2 rounded-lg bg-rose-500/10 flex-shrink-0 mt-0.5">
              <AlertTriangle size={20} className="text-rose-500" />
            </div>
            <div className="min-w-0">
              <h3 className="text-white font-semibold text-sm mb-1">{title}</h3>
              <p className="text-slate-400 text-sm leading-relaxed">{message}</p>
            </div>
          </div>
        </div>

        <div className="flex justify-end gap-2 px-6 pb-5">
          {/* autoFocus on Cancel so Enter doesn't accidentally confirm */}
          <button
            autoFocus
            onClick={onCancel}
            className="px-4 py-2 text-sm text-slate-300 bg-slate-800 hover:bg-slate-700 rounded-lg transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            className="px-4 py-2 text-sm text-white bg-rose-600 hover:bg-rose-700 rounded-lg transition-colors font-semibold"
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
};

export default ConfirmDialog;
