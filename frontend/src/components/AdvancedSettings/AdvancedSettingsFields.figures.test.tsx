import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import { TooltipProvider } from '@/components/ui/tooltip';
import { WORKSPACE_DEFAULT_ADVANCED } from '@/pages/Workspace/constants';
import { buildConfig } from '@/pages/Workspace/helpers';
import {
  AdvancedSettingsFields,
  DEFAULT_ADVANCED_SETTINGS,
} from './AdvancedSettingsFields';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

jest.mock('@/components/ModelSelector', () => ({
  ModelSelector: () => null,
}));

jest.mock('@/components/InitialSchemaEditor/InitialSchemaEditor', () => ({
  __esModule: true,
  default: () => null,
}));

describe('Per-project PDF figure extraction toggle', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it('defaults to enabled and emits enableFigureExtraction patch on click', () => {
    const onChange = jest.fn();

    act(() => {
      root.render(
        <TooltipProvider>
          <AdvancedSettingsFields
            value={DEFAULT_ADVANCED_SETTINGS}
            onChange={onChange}
            developerMode={false}
            allowLlmConfig={false}
            providers={['gemini']}
          />
        </TooltipProvider>,
      );
    });

    const checkbox = container.querySelector('#adv-enable-figures') as HTMLButtonElement | null;
    expect(checkbox).not.toBeNull();
    expect(checkbox?.getAttribute('aria-checked')).toBe('true');

    act(() => {
      checkbox?.click();
    });

    expect(onChange).toHaveBeenCalledWith({ enableFigureExtraction: false });
  });

  it('propagates enable_figure_extraction into buildConfig', () => {
    const defaultCfg = buildConfig('What was decided?', '');
    expect(defaultCfg.enable_figure_extraction).toBe(true);

    const disabledCfg = buildConfig('What was decided?', '', {
      ...WORKSPACE_DEFAULT_ADVANCED,
      enableFigureExtraction: false,
    });
    expect(disabledCfg.enable_figure_extraction).toBe(false);
  });
});

