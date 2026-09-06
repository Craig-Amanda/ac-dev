export type FieldShapeInfo = {
    summary: string;
    formattedShape: unknown;
    rawShape: unknown;
    notes?: string;
    /**
     * The format/relationship object to send to knack_create_field / knack_update_field
     * when creating or editing this field type — as opposed to formattedShape/rawShape,
     * which describe what a record's *value* looks like once the field exists.
     */
    definitionShape?: string;
    definitionNotes?: string;
};

export const KNACK_CONDITIONAL_RULES_SHAPE = {
    summary:
        'Conditional field rules (dynamic default values) live in the "rules" array on a field definition, not in "format". Verified against a live app on 2026-08-14, and re-checked against a 1,911-field schema export from a second app on 2026-09-03.',
    copyAnotherFieldShape:
        '{ "key": "1", "values": [{ "type": "record", "field": "<target_field_key>", "input": "<source_field_key>", "value": "", "connection_field": null }], "criteria": [{ "field": "<test_field_key>", "value": "No", "operator": "is", "value_type": "custom", "value_field": "<auto_increment_field_key>" }] }',
    setFixedValueShape:
        '{ "key": "1", "values": [{ "type": "value", "field": "<target_field_key>", "value": 1, "connection_field": null }], "criteria": [{ "field": "<test_field_key>", "value": "Cat 1", "operator": "is", "value_type": "custom", "value_field": "<auto_increment_field_key>" }] }',
    notes: [
        'To copy another field\'s value, put the source field key in values[].input, not values[].value — putting it in "value" fails silently. Corroborated across 126 record-type values: 117 carried input with an empty value, 9 carried both, none relied on value alone.',
        'criteria[].value_type decides whether value_field means anything, and this note previously said its purpose was unclear. With value_type "field" (7 of 223 observed) value_field names the field being compared against, and was never the auto_increment key. With value_type "custom" (216) the literal in "value" is used and value_field is inert — which is why it usually holds the object\'s auto_increment key, a Builder default rather than a meaningful target. So mirroring an auto_increment key stays harmless, but a field-to-field comparison needs value_type "field" and a real target.',
        'That auto_increment observation was also narrower than it read: 200 of 223 occurrences, not all of them. The other 23 pointed at connection and concatenation fields while still on value_type "custom", so do not treat an auto_increment value_field as a required shape.',
        'A rule\'s "key" is a string when present, is not sequential (28 of 39 rule-bearing fields had keys that were not 1..n), and can be absent altogether — 17 of 236 rules carried no key at all. Builder-assigned either way; never compute it.',
        'values[] entries carry { type, field, value, connection_field } and, for type "record", an "input". One observation added an "action" key, so treat the set as open rather than closed.',
        'Conditional rules only re-evaluate on record save. A schema change alone will not re-run rules against existing records; force a save (e.g. write an unrelated field) to see the effect.',
        'The counts above come from a second app: a schema export of 1,911 fields read on 2026-09-03, which is what corrected the two claims in this list that were true of their original sample and not in general.',
    ],
};

export const KNACK_FIELD_SHAPES: Record<string, FieldShapeInfo> = {
    short_text: {
        summary: 'Plain string.',
        formattedShape: '"Hello World"',
        rawShape: '"Hello World"',
    },
    paragraph_text: {
        summary: 'Multi-line text value.',
        formattedShape: '"Line one<br />Line two"',
        rawShape: '"Line one\\nLine two"',
        notes: 'Formatted output can contain HTML line breaks. Raw preserves newline characters.',
    },
    email: {
        summary: 'Email value with optional label metadata.',
        formattedShape:
            '"<a href="mailto:user@example.com">user@example.com</a>"',
        rawShape: '{ "email": "user@example.com", "label": "Work" }',
        notes: 'Formatted output is typically a mailto anchor. Raw is an object with email and label.',
    },
    phone: {
        summary: 'Phone value with structured number parts.',
        formattedShape: '"<a href="tel:07543423538">07543423538</a>"',
        rawShape:
            '{ "area": null, "number": "07543423538", "ext": null, "full": "07543423538", "country": null, "formatted": "07543423538" }',
        notes: 'Formatted output is typically a tel anchor. Raw is an object containing number parts and preformatted variants.',
    },
    number: {
        summary: 'Numeric value.',
        formattedShape: '"$1,234.50"',
        rawShape: 1234.5,
        notes: 'Raw is a JS number. Formatted output depends on the field display settings and may include prefixes or suffixes.',
    },
    currency: {
        summary: 'Currency value.',
        formattedShape: '"$1,234.56"',
        rawShape: '"1234.56"',
        notes: 'Formatted includes currency symbols and separators. Raw is commonly a numeric string rather than a JS number.',
    },
    auto_increment: {
        summary: 'Auto-incrementing integer.',
        formattedShape: '"42"',
        rawShape: 42,
    },
    boolean: {
        summary: 'Yes/No field. Also referred to as yes_no.',
        formattedShape: '"Yes"',
        rawShape: true,
        notes: 'Raw is a JS boolean. Formatted is typically "Yes" or "No".',
    },
    yes_no: {
        summary: 'Yes/No boolean field.',
        formattedShape: '"Yes"',
        rawShape: true,
        notes: 'Alias for boolean. Raw is a JS boolean.',
    },
    rating: {
        summary: 'Numeric rating value.',
        formattedShape: '"3"',
        rawShape: 3,
    },
    equation: {
        summary:
            'Computed equation result whose shape depends on the configured return type.',
        formattedShape: '"(-42.00)" | "05/01/2026"',
        rawShape:
            '42 | "2026-01-05" | { "date": "01/05/2026", "date_formatted": "05/01/2026", "unix_timestamp": 1767571200000 }',
        notes: 'Equation fields can return numbers, plain strings, or date-like values depending on configuration. For date-returning equations, raw may be a scalar date string or a structured date object, while formatted applies the field display format.',
        definitionShape:
            '{ "equation": "{field_1387.field_1761}*{field_1394.field_439}+{field_1387.field_1762}*{field_1394.field_440}", "equation_type": "numeric", "date_type": "", "date_result": "", "date_format": "mm/dd/yyyy", "time_format": "Ignore Time", "count_field": "Connection", "formula_field": "Field", "rounding": "none", "precision": "2", "mark_decimal": ".", "mark_thousands": "", "pre": "£", "post": "", "format": "" }',
        definitionNotes:
            'Reference local fields as {field_key} and fields on connected records as {connection_field_key.target_field_key} — the qualified form only, since bare names like {Cat 1 Price} have been observed to resolve correctly on one read and silently to 0 on the next with no error either way. One equation can cross more than one connection field on the same object. Only many-to-one / one-to-one connections can be crossed this way; many-to-many connections are not exposed to equations. Equation values recalculate on record save — allow ~15s after a schema change before asserting against them, and always assert against a known non-zero expected value, since an unresolved reference returns 0 rather than an error and a vacuous test would still pass. knack_create_field / knack_update_field now reject or warn on unresolvable {...} tokens before the write reaches the app. Corroborated on a 1,911-field export from a second app on 2026-09-03: all 185 equation tokens used the field-key form and none were name-based, and of 39 connections crossed by an equation none was many-to-many — 38 one-to-many and 1 one-to-one. The date_type, date_result, date_format and time_format keys in the shape above are optional: 85 of 90 equation fields carried them and 5 did not.',
    },
    sum: {
        summary: 'Numeric aggregate (sum of connected records).',
        formattedShape: '"100"',
        rawShape: 100,
    },
    count: {
        summary: 'Numeric count of connected records.',
        formattedShape: '"5"',
        rawShape: 5,
    },
    average: {
        summary: 'Numeric average of connected records.',
        formattedShape: '"3.5"',
        rawShape: 3.5,
    },
    min: {
        summary: 'Minimum value from connected records.',
        formattedShape: '"1"',
        rawShape: 1,
    },
    max: {
        summary: 'Maximum value from connected records.',
        formattedShape: '"10"',
        rawShape: 10,
    },
    concatenation: {
        summary: 'Concatenated string from other fields.',
        formattedShape: '"John Smith - Manager"',
        rawShape: '"John Smith - Manager"',
    },
    name: {
        summary: 'Full name composed of title, first, middle, last, suffix.',
        formattedShape: '"John A. Smith"',
        rawShape:
            '{ "title": "Mr", "first": "John", "middle": "A", "last": "Smith", "full": "John A. Smith" }',
        notes: 'Raw is an object with individual name parts. Optional keys such as middle or suffix may be omitted or blank.',
    },
    address: {
        summary: 'Postal address with geocoordinates.',
        formattedShape: '"123 Main St<br />Springfield, IL 62701"',
        rawShape:
            '{ "street": "123 Main St", "street2": null, "city": "Springfield", "state": "IL", "zip": "62701", "country": null, "longitude": null, "latitude": null, "full": "123 Main St Springfield, IL 62701" }',
        notes: 'Formatted output can contain HTML line breaks. Raw includes address components plus a full string; geo fields are often null.',
    },
    date_time: {
        summary: 'Date and/or time value.',
        formattedShape: '"01/15/2024 10:30 am"',
        rawShape:
            '{ "date": "01/15/2024", "date_formatted": "January 15, 2024", "hours": "10", "minutes": "30", "am_pm": "AM", "unix_timestamp": 1705316400000, "iso_timestamp": "2024-01-15T10:30:00.000Z", "timestamp": "01/15/2024 10:30 am" }',
        notes: 'Formatted output depends on the field configuration and may be date-only, time-only, or a range. Raw for native date/time fields is typically a structured object with date/time parts, proper_* timestamp keys, and an optional to object for ranges rather than a scalar string.',
    },
    timer: {
        summary: 'Time tracking timer with start/stop times.',
        formattedShape: '"2:30:00"',
        rawShape:
            '{ "times": [{ "from": { "date": "01/15/2024", "hours": "10", "minutes": "00", "am_pm": "AM" }, "to": { "date": "01/15/2024", "hours": "12", "minutes": "30", "am_pm": "PM" } }], "running": false, "hours": 2.5, "minutes": 150, "seconds": 9000 }',
        notes: 'Formatted is human-readable elapsed time. Raw contains an array of from/to time pairs plus totals.',
    },
    multiple_choice: {
        summary: 'One or more selected options.',
        formattedShape: '"Option A, Option B"',
        rawShape: '"Option A" | ["Option A", "Option B"]',
        notes: 'Raw is a string for single-select controls and an array for multi-select controls. Formatted is a display string.',
    },
    connection: {
        summary: 'Reference to one or more records in another object.',
        formattedShape:
            '"<span class="abc123def456" data-kn="connection-value">Record Label A</span>"',
        rawShape:
            '[{ "id": "abc123def456", "identifier": "Record Label A" }, { "id": "789xyz", "identifier": "Record Label B" }]',
        notes: 'Raw is an array of objects with id and identifier. Formatted output is HTML, usually one span per connected record, not a plain comma-joined string.',
        definitionShape:
            '{ "relationship": { "object": "object_12", "has": "one", "belongs_to": "many" } }',
        definitionNotes:
            'format.object / relationship.object must be an object key (e.g. object_12), not a name. "has"/"belongs_to" describe cardinality from this object\'s perspective — only many-to-one / one-to-one connections can later be referenced from an equation field; many-to-many connections cannot.',
    },
    file: {
        summary: 'Uploaded file attachment.',
        formattedShape: '"document.pdf"',
        rawShape:
            '{ "id": "abc123", "filename": "document.pdf", "url": "https://...", "thumb_url": null, "size": 204800, "mime_type": "application/pdf" }',
        notes: 'Raw includes the download URL and file metadata.',
    },
    image: {
        summary: 'Uploaded image attachment.',
        formattedShape: '"<img src=\'...\' />"',
        rawShape:
            '{ "id": "abc123", "filename": "photo.jpg", "url": "https://...photo.jpg", "thumb_url": "https://...photo_thumb.jpg", "size": 102400, "mime_type": "image/jpeg" }',
        notes: 'Raw includes full-size and thumbnail URLs. Formatted is an HTML img tag.',
    },
    signature: {
        summary: 'Captured signature.',
        formattedShape: '"<img src="data:image/svg+xml;base64,..." />"',
        rawShape: '{ "svg": "<svg ...></svg>", "base30": "2OZ9jcd..." }',
        notes: 'Observed raw payload contains SVG markup plus a base30 stroke encoding rather than hosted image URLs or timestamp metadata.',
    },
    link: {
        summary: 'Hyperlink with URL and display label.',
        formattedShape: '"<a href=\'https://example.com\'>Example</a>"',
        rawShape: '{ "url": "https://example.com", "label": "Example" }',
        notes: 'Raw has url and label. Formatted is an HTML anchor tag.',
    },
    rich_text: {
        summary: 'HTML rich text content.',
        formattedShape: '"<p>Hello <strong>World</strong></p>"',
        rawShape: '"<p>Hello <strong>World</strong></p>"',
        notes: 'Both formatted and raw are HTML strings.',
    },
    user_roles: {
        summary: 'User role assignments (array of role names).',
        formattedShape: '"Admin, Manager"',
        rawShape: '["Admin", "Manager"]',
        notes: 'Raw is an array of role name strings.',
    },
    password: {
        summary: 'Password validation status only (never the actual password).',
        formattedShape: '""',
        rawShape: '{ "validation": "good" }',
        notes: 'Knack never returns the password value. Raw only indicates validation strength.',
    },
};

export function getFieldShapeInfo(fieldType: string): FieldShapeInfo | null {
    return KNACK_FIELD_SHAPES[fieldType.toLowerCase()] || null;
}
