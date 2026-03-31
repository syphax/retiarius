"""
Word pool for generating human-friendly scenario IDs.

Picks a random common 5-letter English word, unique within a project.
"""

import random
from typing import Set

# ~500 common, memorable, easy-to-spell 5-letter words.
# No profanity, no obscure words — these should work as scenario IDs.
WORDS = [
    "about", "above", "acorn", "adapt", "admit", "adopt", "agent", "agree",
    "alarm", "album", "alert", "alien", "align", "alive", "allow", "alone",
    "along", "alpha", "alter", "amber", "ample", "angel", "anger", "angle",
    "ankle", "anvil", "apple", "arena", "arise", "armor", "aroma", "arrow",
    "asset", "atlas", "attic", "audio", "avoid", "awake", "award", "azure",
    "bacon", "badge", "baker", "basin", "batch", "beach", "beard", "beast",
    "began", "begin", "being", "below", "bench", "berry", "birth", "black",
    "blade", "blame", "blank", "blast", "blaze", "bleed", "blend", "bless",
    "blind", "block", "bloom", "blown", "board", "bonus", "booth", "bound",
    "brace", "brain", "brand", "brass", "brave", "bread", "break", "breed",
    "brick", "bride", "brief", "bring", "broad", "brook", "brown", "brush",
    "build", "burst", "buyer", "cabin", "cable", "camel", "candy", "cargo",
    "carry", "catch", "cedar", "chain", "chair", "chalk", "charm", "chase",
    "cheap", "cheek", "chess", "chief", "child", "chips", "choir", "chunk",
    "cider", "civil", "claim", "clash", "class", "clean", "clear", "clerk",
    "click", "cliff", "climb", "cling", "clock", "close", "cloth", "cloud",
    "coach", "coast", "color", "comet", "coral", "could", "count", "court",
    "cover", "craft", "crane", "crash", "cream", "creek", "crest", "crisp",
    "cross", "crowd", "crown", "crush", "curve", "cycle", "daily", "dairy",
    "dance", "delta", "demon", "depot", "depth", "derby", "diner", "disco",
    "ditch", "diver", "dodge", "doing", "donor", "doubt", "dough", "draft",
    "drain", "drake", "drape", "drawn", "dream", "dress", "drift", "drill",
    "drink", "drive", "drone", "drove", "dryly", "dusty", "dwarf", "dwell",
    "eagle", "early", "earth", "elbow", "elder", "elect", "elite", "ember",
    "empty", "enjoy", "enter", "entry", "equal", "error", "essay", "event",
    "every", "exact", "exert", "exile", "exist", "extra", "fable", "facet",
    "faint", "fairy", "faith", "feast", "fence", "ferry", "fever", "fiber",
    "field", "fifth", "fifty", "fight", "final", "first", "flame", "flash",
    "flask", "fleet", "fling", "float", "flood", "floor", "flora", "flour",
    "flown", "fluid", "flute", "focal", "focus", "force", "forge", "forth",
    "forum", "found", "frame", "frank", "fresh", "front", "frost", "fruit",
    "gauge", "ghost", "giant", "given", "glaze", "gleam", "globe", "gloom",
    "glory", "gloss", "glove", "going", "grace", "grade", "grain", "grand",
    "grant", "grape", "graph", "grasp", "grass", "grave", "great", "green",
    "greet", "grief", "grill", "grind", "groan", "groom", "gross", "group",
    "grove", "grown", "guard", "guess", "guest", "guide", "guild", "habit",
    "harsh", "haven", "hazel", "heart", "heavy", "hence", "heron", "hilly",
    "hoist", "honey", "honor", "horse", "hotel", "house", "human", "humor",
    "ideal", "image", "imply", "inbox", "index", "indie", "inner", "input",
    "ivory", "jewel", "joint", "joker", "judge", "juice", "juicy", "jumbo",
    "kayak", "knack", "kneel", "knife", "knock", "known", "label", "labor",
    "lance", "large", "laser", "latch", "later", "layer", "learn", "lease",
    "legal", "lemon", "level", "lever", "light", "lilac", "linen", "liner",
    "local", "lodge", "logic", "lotus", "lunar", "lunch", "lyric", "magic",
    "major", "maker", "manor", "maple", "march", "marsh", "match", "maybe",
    "mayor", "melon", "mercy", "merge", "merit", "metal", "meter", "micro",
    "might", "minor", "model", "money", "month", "moose", "moral", "motor",
    "mount", "mouse", "mouth", "movie", "mural", "music", "naive", "naval",
    "nerve", "never", "night", "noble", "noise", "north", "noted", "novel",
    "nurse", "nylon", "oasis", "ocean", "offer", "olive", "onset", "opera",
    "orbit", "order", "other", "outer", "owner", "oxide", "ozone", "paint",
    "panel", "paper", "paste", "patch", "pause", "peace", "peach", "pearl",
    "pedal", "penny", "perch", "phase", "photo", "piano", "pilot", "pinch",
    "pitch", "pixel", "pizza", "place", "plaid", "plain", "plane", "plant",
    "plate", "plaza", "plead", "plier", "plumb", "plume", "plush", "point",
    "polar", "polka", "pound", "power", "press", "price", "pride", "prime",
    "print", "prior", "prism", "prize", "probe", "proof", "prose", "proud",
    "proxy", "pulse", "pupil", "purse", "queen", "quest", "quick", "quiet",
    "quilt", "quota", "quote", "radar", "radio", "rally", "ranch", "range",
    "rapid", "raven", "reach", "ready", "realm", "rebel", "reign", "relay",
    "relic", "rider", "ridge", "rifle", "right", "risen", "risky", "rival",
    "river", "robin", "robot", "rocky", "roger", "rouge", "round", "route",
    "royal", "rugby", "ruler", "rural", "saint", "salad", "sandy", "satin",
    "sauce", "sauna", "scale", "scene", "scope", "score", "scout", "screw",
    "sedan", "serve", "seven", "shade", "shall", "shape", "share", "shark",
    "sharp", "shelf", "shell", "shift", "shine", "shirt", "shock", "shore",
    "short", "shown", "sight", "sigma", "since", "sixth", "sixty", "skill",
    "skull", "slate", "sleep", "slice", "slide", "slope", "smart", "smile",
    "smoke", "snack", "snake", "solar", "solid", "solve", "sound", "south",
    "space", "spare", "spark", "speak", "spear", "speed", "spend", "spice",
    "spike", "spine", "spoke", "sport", "spray", "squad", "stack", "staff",
    "stage", "stain", "stair", "stake", "stale", "stand", "start", "state",
    "stays", "steam", "steel", "steep", "steer", "stick", "still", "stock",
    "stone", "stood", "store", "storm", "story", "stove", "strap", "straw",
    "stray", "strip", "stuck", "study", "stuff", "style", "sugar", "suite",
    "super", "surge", "swamp", "swarm", "swear", "sweet", "swept", "swift",
    "swing", "sword", "syrup", "table", "taken", "taste", "tempo", "theft",
    "theme", "thick", "thing", "think", "thorn", "those", "three", "throw",
    "thumb", "tiger", "tight", "timer", "title", "token", "total", "touch",
    "tough", "tower", "toxic", "trace", "track", "trade", "trail", "train",
    "trait", "treat", "trend", "trial", "tribe", "trick", "tried", "troop",
    "trout", "truck", "truly", "trunk", "trust", "truth", "tulip", "tuner",
    "turbo", "twice", "twist", "ultra", "uncle", "under", "union", "unite",
    "unity", "until", "upper", "upset", "urban", "usage", "usual", "utter",
    "valid", "value", "valve", "vapor", "vault", "verse", "video", "vigor",
    "vinyl", "viola", "viral", "visit", "visor", "vista", "vital", "vivid",
    "vocal", "voice", "voter", "vowel", "wagon", "waste", "watch", "water",
    "weary", "weave", "wedge", "wheat", "wheel", "where", "while", "white",
    "whole", "widen", "witch", "woman", "world", "worse", "worst", "worth",
    "would", "wound", "wrist", "write", "yacht", "yield", "young", "youth",
    "zebra", "zippy",
]


def generate_scenario_id(existing_ids: Set[str]) -> str:
    """Pick a random 5-letter word not already used in `existing_ids`.

    If all words are taken (unlikely with 500+ words), appends a digit.
    """
    # Compare case-insensitively against existing IDs
    existing_lower = {eid.lower() for eid in existing_ids}
    available = [w for w in WORDS if w not in existing_lower]
    if available:
        return random.choice(available).upper()

    # Fallback: append digits to a random word
    for _ in range(1000):
        base = random.choice(WORDS)
        for suffix in range(2, 100):
            candidate = f"{base}{suffix}"
            if candidate not in existing_lower:
                return candidate.upper()

    # Should never reach here
    import uuid
    return uuid.uuid4().hex[:8].upper()


def next_clone_name(original_name: str, existing_names: Set[str]) -> str:
    """Generate clone name: '<original> clone 01', '...clone 02', etc."""
    for i in range(1, 100):
        candidate = f"{original_name} clone {i:02d}"
        if candidate not in existing_names:
            return candidate
    return f"{original_name} clone"
