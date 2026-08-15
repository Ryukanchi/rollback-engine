/**
 * Seeded Pseudo-Random Number Generator using Mulberry32 algorithm.
 * Guarantees 100% deterministic, reproducible pseudo-random streams across all platforms.
 */
class SeededRandom {
  #seed;
  #state;

  constructor(seed) {
    let numericSeed;
    if (typeof seed === "number") {
      numericSeed = Math.floor(seed) >>> 0;
    } else if (typeof seed === "string") {
      numericSeed = SeededRandom.hashString(seed);
    } else {
      numericSeed = (Date.now() ^ (Math.random() * 0x100000000)) >>> 0;
    }

    if (numericSeed === 0) {
      numericSeed = 1;
    }

    this.#seed = numericSeed;
    this.#state = numericSeed;
  }

  static hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
    }
    return (hash >>> 0) || 1;
  }

  getSeed() {
    return this.#seed;
  }

  /**
   * Generates a deterministic float in range [0, 1)
   */
  next() {
    let t = (this.#state += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /**
   * Generates integer in range [min, max] inclusive
   */
  nextInt(min, max) {
    const lo = Math.ceil(min);
    const hi = Math.floor(max);
    return lo + Math.floor(this.next() * (hi - lo + 1));
  }

  /**
   * Returns a boolean with probability `chance`
   */
  nextBoolean(chance = 0.5) {
    return this.next() < chance;
  }

  /**
   * Picks one element from array
   */
  pick(array) {
    if (!array || array.length === 0) return undefined;
    const index = this.nextInt(0, array.length - 1);
    return array[index];
  }

  /**
   * Shuffles an array deterministically (Fisher-Yates)
   */
  shuffle(array) {
    const clone = [...array];
    for (let i = clone.length - 1; i > 0; i--) {
      const j = this.nextInt(0, i);
      [clone[i], clone[j]] = [clone[j], clone[i]];
    }
    return clone;
  }

  /**
   * Generates a random alphanumeric string of given length
   */
  nextString(length = 8, alphabet = "abcdefghijklmnopqrstuvwxyz0123456789") {
    let result = "";
    for (let i = 0; i < length; i++) {
      result += alphabet[this.nextInt(0, alphabet.length - 1)];
    }
    return result;
  }

  /**
   * Creates an independent child PRNG derived from the current stream
   */
  fork() {
    return new SeededRandom(this.nextInt(1, 0x7fffffff));
  }
}

module.exports = {
  SeededRandom,
};
