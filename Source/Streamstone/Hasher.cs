namespace Streamstone
{
    /// <summary>
    ///     An implementation of Google's XXHash algorithm taken from 
    ///     https://github.com/EventStore/EventStore/blob/master/src/EventStore.Core/Index/Hashes/XXHashUnsafe.cs
    /// </summary>
    class Hasher
    {
        const uint Seed = 0xc58f1a7b;

        const uint PRIME1 = 2654435761U;
        const uint PRIME2 = 2246822519U;
        const uint PRIME3 = 3266489917U;
        const uint PRIME4 = 668265263U;
        const int  PRIME5 = 0x165667b1;

        internal static unsafe uint Hash(string s)
        {
            fixed (char* input = s)
                return Hash((byte*) input, (uint) s.Length * sizeof(char), Seed);
        }

        static unsafe uint Hash(byte* data, uint len, uint seed)
        {
            if (len < 16)
                return HashSmall(data, len, seed);

            var v1 = seed + PRIME1;
            var v2 = v1 * PRIME2 + len;
            var v3 = v2 * PRIME3;
            var v4 = v3 * PRIME4;

            var p = (uint*)data;
            var limit = (uint*)(data + len - 16);

            while (p < limit)
            {
                v1 += Rotl32(v1, 13); v1 *= PRIME1; v1 += *p; p++;
                v2 += Rotl32(v2, 11); v2 *= PRIME1; v2 += *p; p++;
                v3 += Rotl32(v3, 17); v3 *= PRIME1; v3 += *p; p++;
                v4 += Rotl32(v4, 19); v4 *= PRIME1; v4 += *p; p++;
            }

            p = limit;
            v1 += Rotl32(v1, 17); v2 += Rotl32(v2, 19); v3 += Rotl32(v3, 13); v4 += Rotl32(v4, 11);
            v1 *= PRIME1; v2 *= PRIME1; v3 *= PRIME1; v4 *= PRIME1;
            v1 += *p; p++; v2 += *p; p++; v3 += *p; p++; v4 += *p;
            v1 *= PRIME2; v2 *= PRIME2; v3 *= PRIME2; v4 *= PRIME2;
            v1 += Rotl32(v1, 11); v2 += Rotl32(v2, 17); v3 += Rotl32(v3, 19); v4 += Rotl32(v4, 13);
            v1 *= PRIME3; v2 *= PRIME3; v3 *= PRIME3; v4 *= PRIME3;

            var crc = v1 + Rotl32(v2, 3) + Rotl32(v3, 6) + Rotl32(v4, 9);
            crc ^= crc >> 11;
            crc += (PRIME4 + len) * PRIME1;
            crc ^= crc >> 15;
            crc *= PRIME2;
            crc ^= crc >> 13;
            return crc;
        }

        static unsafe uint HashSmall(byte* data, uint len, uint seed)
        {
            var p = data;
            var bEnd = data + len;
            var limit = bEnd - 4;

            var idx = seed + PRIME1;
            uint crc = PRIME5;

            while (p < limit)
            {
                crc += (*(uint*)p) + idx;
                idx++;
                crc += Rotl32(crc, 17) * PRIME4;
                crc *= PRIME1;
                p += 4;
            }

            while (p < bEnd)
            {
                crc += (*p) + idx;
                idx++;
                crc *= PRIME1;
                p++;
            }

            crc += len;

            crc ^= crc >> 15;
            crc *= PRIME2;
            crc ^= crc >> 13;
            crc *= PRIME3;
            crc ^= crc >> 16;

            return crc;
        }

        static uint Rotl32(uint x, int r) => (x << r) | (x >> (32 - r));
    }
}