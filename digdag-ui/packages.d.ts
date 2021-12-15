// duration has no types.
declare module "duration" {
  export declare class Duration {
    constructor(start: Date, end: Date)
    toString(mode?: 0 | 1, threshold?: number): string
  }
}

// uuid/v4 is deprecated so @types/uuid doesn't declare types for v4.
declare module "uuid/v4" {
  export default function(): default
}

// need upgrade to get types
declare module "buffer/" {
  export declare class Buffer {
    constructor(buf: ArrayBufferLike)
    length: number
    toString(): string
  }
}

// js-untar has no types
declare module "js-untar" {
  export default function(u: ArrayBufferLike): Promise<{
    name: string;
    buffer: ArrayBuffer;
  }[]>
}
