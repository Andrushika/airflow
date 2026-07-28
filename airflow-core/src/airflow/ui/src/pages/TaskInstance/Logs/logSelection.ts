/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Map a DOM node inside the virtualized log list to the `data-index` of the
 * row containing it.
 */
export const getRowIndexForNode = (node: Node | null, container: HTMLElement): number | undefined => {
  const element = node instanceof Element ? node : node?.parentElement;
  const row = element?.closest("[data-index]");

  if (!row || !container.contains(row)) {
    return undefined;
  }
  const index = Number(row.getAttribute("data-index"));

  return Number.isInteger(index) ? index : undefined;
};

/**
 * Row indexes to pin so the virtualizer keeps selection-boundary rows
 * mounted. Boundaries map independently (the drag focus may sit off the
 * rows) and a collapsed caret pins too, for shift-click extension.
 */
export const getSelectionPinnedRows = (
  selection: Selection | null,
  container: HTMLElement,
): Array<number> => {
  if (!selection || selection.rangeCount === 0) {
    return [];
  }
  const range = selection.getRangeAt(0);

  return [
    getRowIndexForNode(range.startContainer, container),
    getRowIndexForNode(range.endContainer, container),
  ].filter((index): index is number => index !== undefined);
};

type DragClampOptions = {
  container: HTMLElement;
  pointerY: number;
  selection: Selection;
};

/**
 * Boundary to re-extend a drag selection to while the pointer is vertically
 * outside the container: the selection follows the nearest mounted row edge
 * in the drag direction, matching how dragging past a page edge keeps
 * selecting. Chrome cannot do this itself here — during autoscroll its
 * hit-tests land on unmounted spacer space and intermittently collapse the
 * selection.
 */
export const getDragClampTarget = ({
  container,
  pointerY,
  selection,
}: DragClampOptions): { node: Node; offset: number } | undefined => {
  if (selection.rangeCount === 0) {
    return undefined;
  }
  const anchorRow = getRowIndexForNode(selection.anchorNode, container);

  if (anchorRow === undefined) {
    return undefined;
  }
  const rect = container.getBoundingClientRect();
  const rows = container.querySelectorAll("[data-index]");
  const [firstRow] = rows;
  const lastRow = rows[rows.length - 1];

  if (lastRow === undefined || firstRow === undefined) {
    return undefined;
  }
  // Never re-extend to the current focus — breaks the selectionchange recursion.
  const clampTo = (node: Element, offset: number) =>
    selection.focusNode === node && selection.focusOffset === offset ? undefined : { node, offset };

  if (pointerY >= rect.bottom) {
    return clampTo(lastRow, lastRow.childNodes.length);
  }
  if (pointerY <= rect.top) {
    return clampTo(firstRow, 0);
  }

  return undefined;
};

/**
 * Merge selection-pinned row indexes into the virtualizer's default render
 * range. Rows holding selection boundaries must stay mounted while the user
 * scrolls — unmounting a boundary node collapses the browser selection.
 */
export const mergePinnedIndexes = (
  defaultIndexes: Array<number>,
  pinnedIndexes: Array<number>,
  count: number,
): Array<number> => {
  const validPins = pinnedIndexes.filter((index) => index >= 0 && index < count);

  if (validPins.length === 0) {
    return defaultIndexes;
  }

  return [...new Set([...validPins, ...defaultIndexes])].sort((first, second) => first - second);
};
