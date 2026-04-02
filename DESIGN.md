# Design System Specification: The Terminal Architecture

## 1. Overview & Creative North Star
**Creative North Star: "The Digital Command Center"**

This design system moves away from the "friendly SaaS" aesthetic and toward a high-fidelity, hardcore developer environment. The goal is to evoke the focused, high-stakes atmosphere of a mission-critical terminal. We achieve this through **Architectural Brutalism**—using razor-sharp 0px radiuses, hyper-focused typography, and tonal layering to create depth without sacrificing the "flat" efficiency of a CLI.

The design breaks the standard web grid by utilizing intentional asymmetry, mimicking how a developer tiles windows on a 4K monitor. Elements should feel like they are docked, snapped, or floating within a high-end IDE.

---

## 2. Colors & Surface Logic

### The Palette
We utilize a cold, deep-space foundation with neon-injected highlights.
*   **Background / Surface:** `#0e141a` (The void)
*   **Primary (Accent):** `#81e6fa` / `#63cadd` (The glow)
*   **Secondary (Subtle):** `#b4c9df` / `#93a7bd` (The metadata)

### The "No-Line" Rule
Traditional 1px borders are strictly prohibited for structural sectioning. To separate content, use the **Surface Hierarchy**:
*   **Base Layer:** `surface` (#0e141a)
*   **Section Break:** `surface-container-low` (#161c23)
*   **Interactive Cards:** `surface-container-high` (#252a32)

### The Glass & Gradient Rule
For the "Ferry" aspect of the brand—representing data in motion—use **Glassmorphism**.
*   **Floating Panels:** Apply `surface-container-highest` at 60% opacity with a `20px` backdrop blur.
*   **Visual Soul:** Main CTAs should not be flat. Use a linear gradient from `primary` (#81e6fa) to `primary-container` (#63cadd) at a 135-degree angle to simulate light emitting from a screen.

---

## 3. Typography: Technical Authority

The system relies on the tension between the wide, architectural curves of **Space Grotesk** and the rigid, functional precision of **IBM Plex Mono**.

*   **Display & Headlines (Space Grotesk):** Use these for high-level concepts. Space Grotesk's technical geometry feels modern and authoritative. 
    *   *Scale:* `display-lg` (3.5rem) for hero statements; `headline-md` (1.75rem) for section titles.
*   **The Command Layer (IBM Plex Mono):** All technical data, code snippets, and "utility" labels must use IBM Plex Mono. This is the industry standard for clarity.
    *   *Scale:* `title-sm` (1rem) for input fields; `label-sm` (0.6875rem) for micro-metadata.
*   **Body (Inter):** For long-form descriptions, Inter provides the necessary legibility to balance the more stylistic fonts.

---

## 4. Elevation & Depth: Tonal Layering

We do not use drop shadows to indicate "elevation" in the traditional sense; we use **Luminance Stacking**.

*   **The Layering Principle:** A "floating" terminal window is defined by being one step brighter than the layer beneath it. Place a `surface-container-highest` element on top of a `surface-dim` background to create a "lift" effect.
*   **The Ghost Border:** If high-contrast separation is required (e.g., in a complex CLI output), use a `1px` border using `outline-variant` (#3e494b) at **15% opacity**. It should be felt, not seen.
*   **Ambient Glow:** For the primary action state, replace a shadow with a subtle `primary` glow.
    *   *Spec:* `box-shadow: 0 0 30px rgba(99, 202, 221, 0.15);`

---

## 5. Components

### The "Command" Button (Primary)
*   **Shape:** 0px border radius (Sharp).
*   **Style:** `primary-container` background with `on-primary-container` text.
*   **Visual Cue:** A `>` character (IBM Plex Mono) should precede the text label to mimic a terminal prompt.
*   **Hover:** Shift background to `primary` and add the "Ambient Glow."

### Terminal Panels (Cards)
*   **Constraint:** No dividers. Use `0.9rem` (spacing-4) of vertical whitespace to separate content blocks.
*   **Header:** A `surface-container-highest` bar at the top with three "window control" circles (use `outline` color) to reinforce the OS aesthetic.

### Step Indicators (Workflow)
*   **Logic:** Instead of circles, use high-visibility vertical bars.
*   **Active State:** `primary` (#81e6fa) with a high-glow effect.
*   **Inactive State:** `outline-variant` (#3e494b).

### Input Fields
*   **Style:** Underline only (2px). No containing box.
*   **Prompt:** Prefix every input with `$` or `db-ferry ~`.
*   **Caret:** Use a flashing block cursor (`primary` color) at the end of active text.

---

## 6. Do's and Don'ts

### Do:
*   **Embrace Asymmetry:** Align text to the left but allow code blocks to bleed off the right edge of the container to suggest "infinite" terminal space.
*   **Use Mono for Numbers:** All metrics, dates, and counts must be in **IBM Plex Mono**.
*   **Respect the Grid:** Use the `spacing-8` (1.75rem) as your "standard" gutter for a breathable, editorial feel.

### Don't:
*   **No Rounded Corners:** Never use `border-radius`. The aesthetic must remain "Hardcore" and "Sharp."
*   **No Generic Icons:** Avoid soft, rounded icon sets. Use thin-stroke, geometric icons (e.g., Lucide or custom SVG) with sharp joins.
*   **No Centered Text:** Real terminals are left-aligned. Keep 95% of your layout left-aligned to maintain the developer-first mental model.