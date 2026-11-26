import * as fs from 'fs';
import * as path from 'path';
import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import { ScheMatiQLogo } from './ScheMatiQLogo';

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

/**
 * The mark exists twice: public/logo.svg (favicon, README) and this component
 * (in-app, with a per-instance gradient id). This test fails if the two drift.
 */

type Shape = { tag: string; attrs: Record<string, string> };

// Attributes that differ on purpose between the static file and the component.
const IGNORED_ATTRS = new Set(['id', 'class', 'role', 'aria-label', 'aria-hidden', 'xmlns']);

function shapesOf(svg: Element): Shape[] {
  return Array.from(svg.querySelectorAll('*')).map((el) => {
    const attrs: Record<string, string> = {};
    for (const { name, value } of Array.from(el.attributes)) {
      if (IGNORED_ATTRS.has(name)) continue;
      // Gradient ids are per-instance in the component, so compare references only.
      attrs[name] = value.replace(/url\(#[^)]*\)/g, 'url(#gradient)');
    }
    return { tag: el.tagName.toLowerCase(), attrs };
  });
}

function rootAttrsOf(svg: Element): Record<string, string | null> {
  return { viewBox: svg.getAttribute('viewBox'), fill: svg.getAttribute('fill') };
}

describe('ScheMatiQLogo', () => {
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

  const draw = (node: React.ReactElement) => {
    // root.render is react-dom's, not Testing Library's; the rule only matches the name.
    // eslint-disable-next-line testing-library/no-unnecessary-act
    act(() => {
      root.render(node);
    });
  };

  const file = fs.readFileSync(path.join(__dirname, '../../../public/logo.svg'), 'utf8');
  const staticSvg = new DOMParser().parseFromString(file, 'image/svg+xml').documentElement;

  it('matches the geometry and colors of public/logo.svg', () => {
    draw(<ScheMatiQLogo />);
    const componentSvg = container.querySelector('svg');
    expect(componentSvg).not.toBeNull();
    expect(rootAttrsOf(componentSvg!)).toEqual(rootAttrsOf(staticSvg));
    expect(shapesOf(componentSvg!)).toEqual(shapesOf(staticSvg));
  });

  it('gives each instance its own gradient id', () => {
    draw(
      <>
        <ScheMatiQLogo />
        <ScheMatiQLogo />
      </>,
    );
    const ids = Array.from(container.querySelectorAll('linearGradient')).map((g) => g.id);
    expect(ids).toHaveLength(2);
    expect(new Set(ids).size).toBe(2);
    for (const svg of Array.from(container.querySelectorAll('svg'))) {
      const gradientId = svg.querySelector('linearGradient')!.id;
      expect(svg.querySelector('rect')!.getAttribute('stroke')).toBe(`url(#${gradientId})`);
    }
  });

  it('is decorative by default and labelled when a label is given', () => {
    draw(<ScheMatiQLogo />);
    expect(container.querySelector('svg')!.getAttribute('aria-hidden')).toBe('true');
    draw(<ScheMatiQLogo label="ScheMatiQ" />);
    const svg = container.querySelector('svg')!;
    expect(svg.getAttribute('role')).toBe('img');
    expect(svg.getAttribute('aria-label')).toBe('ScheMatiQ');
    expect(svg.getAttribute('aria-hidden')).toBeNull();
  });
});
