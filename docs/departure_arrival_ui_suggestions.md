# Departure vs Arrival Panel Distinction

Operational tweaks below focus on instant recognition, fast scanning, and low-risk execution.

## High-Impact Quick Wins
- **Color + label band across the top:** Apply a 48–64 px colored banner with large, left-aligned labels (e.g., “DEPARTURE” in blue, “ARRIVAL” in green). Keep the body white/neutral so the band is the strongest cue.
- **Opposite corner anchors:** Put a large direction icon in the leading corner (⬆️ vs ⬇️) and a short label chip in the trailing corner; this keeps cues visible even when zoomed out or printed in grayscale (chip uses outline when color is absent).
- **Sticky subheaders inside cards:** Repeat “Departure” / “Arrival” as a sticky subheader inside scrollable content so context never disappears while scrolling long checklists.

## Layout & Scanning
- **Left/right placement with gutter:** Keep Departure on the left and Arrival on the right with a visible gutter (16–24 px). Align common rows (time, weather, customs, deice, crew) so eyes can move horizontally to compare.
- **Timeline stripe:** Add a thin vertical stripe on the far-left edge of each card. Match the stripe color to the section accent and place key markers (ETD/ETA, cutoff times) on it to anchor the eye.
- **Primary row emphasis:** Give ETD/ETA rows extra padding and a light background tint; show the next required action (e.g., “Customs confirm by 15:00Z”) aligned right.

## Typography & Density
- **Timestamp hierarchy:** Main times in large, bold text; secondary in medium weight. Use monospaced numerals for times to aid quick comparison.
- **Group separators:** Add horizontal rules or subtle shading between logical groups to avoid dense walls of text.

## Interaction & States
- **Jump pills:** Top-level pills/tabs let users jump between cards; highlight the active pill with the matching accent color.
- **State badges:** Right-align status chips such as “Filed,” “Customs Pending,” or “Deice Scheduled” using the section accent. Keep badge text short and uppercase for quick legibility.
- **Progress dots:** For multi-step flows (e.g., customs → deice → crew brief), add three small dots that light up as tasks complete; keep the dot color aligned to the section.

## Accessibility & Reliability
- **Contrast & grayscale:** Meet WCAG AA+; ensure labels and outline chips remain legible if printed or viewed in night mode. Do not rely solely on color—pair every color cue with text or iconography.
- **Consistent ordering:** Mirror the order of subsections across both cards so muscle memory works (time → weather → customs → deice → crew → notes).
- **Touch targets:** Ensure sticky headers, pills, and status chips have 44 px touch targets for tablet/flight deck use.
