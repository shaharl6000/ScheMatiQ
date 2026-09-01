// Dialog showing the ScheMatiQ ACL 2026 citation with a copyable BibTeX block.
// Parent: Workspace (index.tsx). Reachable from the Help menu ("Cite ScheMatiQ")
// and the "Cite" button in the workspace chrome.

import { useState } from 'react';
import { Check, Copy, ExternalLink } from 'lucide-react';

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';

const ACL_URL = 'https://aclanthology.org/2026.acl-demo.22/';
const ARXIV_URL = 'https://arxiv.org/abs/2604.09237';

// Muted link style matching the right-side chrome links (.workspace-chrome-link):
// muted-foreground at rest, foreground on hover — no bright blue.
const LINK_CLASS =
  'inline-flex items-center gap-1 text-muted-foreground hover:text-foreground hover:underline transition-colors';

const BIBTEX = `@inproceedings{levy-etal-2026-schematiq,
    title = "{S}che{M}ati{Q}: From Research Question to Structured Data through Interactive Schema Discovery",
    author = "Levy, Shahar  and
      Habba, Eliya  and
      Mintz, Reshef  and
      Raveh, Barak  and
      Keydar, Renana  and
      Stanovsky, Gabriel",
    editor = "Durrett, Greg  and
      Jian, Ping",
    booktitle = "Proceedings of the 64th Annual Meeting of the {A}ssociation for {C}omputational {L}inguistics (Volume 3: System Demonstrations)",
    month = jul,
    year = "2026",
    address = "San Diego, California, United States",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2026.acl-demo.22/",
    doi = "10.18653/v1/2026.acl-demo.22",
    pages = "220--230",
    ISBN = "979-8-89176-392-0",
}`;

export function CiteDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(BIBTEX);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      // Clipboard may be unavailable (e.g. insecure context); ignore silently.
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      {/* min-w-0 so the BibTeX <pre> below scrolls internally instead of widening
          the (grid) dialog past its max-width. */}
      <DialogContent className="sm:max-w-xl min-w-0">
        <DialogHeader>
          <DialogTitle className="text-lg">Cite ScheMatiQ</DialogTitle>
          <DialogDescription>
            If you use ScheMatiQ in your research, please cite:
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-wrap gap-4 text-sm">
          <a href={ACL_URL} target="_blank" rel="noopener noreferrer" className={LINK_CLASS}>
            ACL Anthology <ExternalLink className="h-3.5 w-3.5" />
          </a>
          <a href={ARXIV_URL} target="_blank" rel="noopener noreferrer" className={LINK_CLASS}>
            arXiv <ExternalLink className="h-3.5 w-3.5" />
          </a>
        </div>

        <div className="min-w-0 space-y-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-muted-foreground">BibTeX</span>
            <Button type="button" size="sm" variant="outline" onClick={handleCopy}>
              {copied ? (
                <>
                  <Check className="h-3.5 w-3.5 mr-1" /> Copied
                </>
              ) : (
                <>
                  <Copy className="h-3.5 w-3.5 mr-1" /> Copy
                </>
              )}
            </Button>
          </div>
          <pre className="w-full max-h-56 overflow-auto rounded-md border bg-muted/50 p-3 text-xs leading-relaxed">
            {BIBTEX}
          </pre>
        </div>
      </DialogContent>
    </Dialog>
  );
}
