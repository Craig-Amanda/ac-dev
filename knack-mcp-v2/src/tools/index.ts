import type { AnyToolDef } from '../registry.js';
import { analysisTools } from './analysis.js';
import { contextTools } from './context.js';
import { fieldTools } from './fields.js';
import { fileTools } from './files.js';
import { recordTools } from './records.js';
import { schemaTools } from './schema.js';
import { viewMutationTools } from './view-mutations.js';
import { viewTools } from './views.js';

/** Every tool, in catalogue order: orientation first, reads before writes. */
export const ALL_TOOLS: AnyToolDef[] = [
    ...contextTools,
    ...schemaTools,
    ...recordTools,
    ...fileTools,
    ...viewTools,
    ...analysisTools,
    ...fieldTools,
    ...viewMutationTools,
];
