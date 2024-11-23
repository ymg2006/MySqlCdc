using System.Buffers;
using System.Text;
using MySqlCdc.Protocol;

namespace MySqlCdc.Columns;

/// <summary>
/// See <a href="https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html">Docs</a>
/// </summary>
internal class ColumnParser
{
    private const int DigitsPerInt = 9;
    private static readonly int[] CompressedBytes = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

    public string ParseNewDecimal(ref PacketReader reader, int metadata)
    {
        var precision = metadata & 0xFF;
        var scale = metadata >> 8;
        var integral = precision - scale;

        var uncompressedIntegral = integral / DigitsPerInt;
        var uncompressedFractional = scale / DigitsPerInt;
        var compressedIntegral = integral - (uncompressedIntegral * DigitsPerInt);
        var compressedFractional = scale - (uncompressedFractional * DigitsPerInt);
        var length =
            (uncompressedIntegral << 2) + CompressedBytes[compressedIntegral] +
            (uncompressedFractional << 2) + CompressedBytes[compressedFractional];

        // Format
        // [1-3 bytes]  [4 bytes]      [4 bytes]        [4 bytes]      [4 bytes]      [1-3 bytes]
        // [Compressed] [Uncompressed] [Uncompressed] . [Uncompressed] [Uncompressed] [Compressed]
        var value = reader.ReadByteArraySlow(length);
        var result = new StringBuilder();

        var negative = (value[0] & 0x80) == 0;
        value[0] ^= 0x80;

        if (negative)
        {
            result.Append('-');
            for (var i = 0; i < value.Length; i++)
                value[i] ^= 0xFF;
        }

        using var memoryOwner = new MemoryOwner(new ReadOnlySequence<byte>(value));
        var buffer = new PacketReader(memoryOwner.Memory.Span);

        var started = false;
        var size = CompressedBytes[compressedIntegral];

        if (size > 0)
        {
            var number = buffer.ReadIntBigEndian(size);
            if (number > 0)
            {
                started = true;
                result.Append(number);
            }
        }
        for (var i = 0; i < uncompressedIntegral; i++)
        {
            var number = buffer.ReadUInt32BigEndian();
            if (started)
            {
                result.Append(number.ToString("D9"));
            }
            else if (number > 0)
            {
                started = true;
                result.Append(number);
            }
        }
        if (!started) // There has to be at least 0
        {
            result.Append('0');
        }

        if (scale > 0)
        {
            result.Append('.');
        }

        size = CompressedBytes[compressedFractional];
        for (var i = 0; i < uncompressedFractional; i++)
        {
            result.Append(buffer.ReadUInt32BigEndian().ToString("D9"));
        }
        if (size > 0)
        {
            result.Append(buffer.ReadIntBigEndian(size).ToString($"D{compressedFractional}"));
        }
        return result.ToString();
    }

    public byte ParseTinyInt(ref PacketReader reader, int metadata) => reader.ReadByte();

    public Int16 ParseSmallInt(ref PacketReader reader, int metadata) => (Int16)reader.ReadUInt16LittleEndian();

    public Int32 ParseMediumInt(ref PacketReader reader, int metadata)
    {
        /* Adjust negative 3-byte number to Int32 */
        return (reader.ReadIntLittleEndian(3) << 8) >> 8;
    }

    public Int32 ParseInt(ref PacketReader reader, int metadata) => (Int32)reader.ReadUInt32LittleEndian();

    public Int64 ParseBigInt(ref PacketReader reader, int metadata) => reader.ReadInt64LittleEndian();

    public float ParseFloat(ref PacketReader reader, int metadata)
    {
        return BitConverter.ToSingle(BitConverter.GetBytes(reader.ReadUInt32LittleEndian()), 0);
    }

    public double ParseDouble(ref PacketReader reader, int metadata)
    {
        return BitConverter.Int64BitsToDouble(reader.ReadInt64LittleEndian());
    }

    public string ParseString(ref PacketReader reader, int metadata)
    {
        int length = metadata < 256 ? reader.ReadByte() : reader.ReadUInt16LittleEndian();
        return reader.ReadString(length);
    }

    public byte[] ParseBlob(ref PacketReader reader, int metadata)
    {
        var length = reader.ReadIntLittleEndian(metadata);
        return reader.ReadByteArraySlow(length);
    }

    public bool[] ParseBit(ref PacketReader reader, int metadata)
    {
        var length = (metadata >> 8) * 8 + (metadata & 0xFF);
        var bitmap = reader.ReadBitmapBigEndian(length);
        Array.Reverse(bitmap);
        return bitmap;
    }

    public int ParseEnum(ref PacketReader reader, int metadata)
    {
        return reader.ReadIntLittleEndian(metadata);
    }

    public long ParseSet(ref PacketReader reader, int metadata)
    {
        return reader.ReadLongLittleEndian(metadata);
    }

    public int ParseYear(ref PacketReader reader, int metadata)
    {
        return 1900 + reader.ReadByte();
    }

    public DateOnly? ParseDate(ref PacketReader reader, int metadata)
    {
        var value = reader.ReadIntLittleEndian(3);

        // Bits 1-5 store the day. Bits 6-9 store the month. The remaining bits store the year.
        var day = value % (1 << 5);
        var month = (value >> 5) % (1 << 4);
        var year = value >> 9;

        if (year == 0 || month == 0 || day == 0)
            return null;

        return new DateOnly(year, month, day);
    }

    public TimeSpan ParseTime(ref PacketReader reader, int metadata)
    {
        var value = (reader.ReadIntLittleEndian(3) << 8) >> 8;

        if (value < 0)
            throw new NotSupportedException("Parsing negative TIME values is not supported in this version");

        var seconds = value % 100;
        value = value / 100;
        var minutes = value % 100;
        value = value / 100;
        var hours = value;
        return new TimeSpan(hours, minutes, seconds);
    }

    public DateTimeOffset ParseTimeStamp(ref PacketReader reader, int metadata)
    {
        long seconds = reader.ReadUInt32LittleEndian();
        return DateTimeOffset.FromUnixTimeSeconds(seconds);
    }

    public DateTime? ParseDateTime(ref PacketReader reader, int metadata)
    {
        var value = reader.ReadInt64LittleEndian();
        var second = (int)(value % 100);
        value = value / 100;
        var minute = (int)(value % 100);
        value = value / 100;
        var hour = (int)(value % 100);
        value = value / 100;
        var day = (int)(value % 100);
        value = value / 100;
        var month = (int)(value % 100);
        value = value / 100;
        var year = (int)value;

        if (year == 0 || month == 0 || day == 0)
            return null;

        return new DateTime(year, month, day, hour, minute, second);
    }

    public TimeSpan ParseTime2(ref PacketReader reader, int metadata)
    {
        var value = reader.ReadIntBigEndian(3);
        var millisecond = ParseFractionalPart(ref reader, metadata) / 1000;

        var negative = ((value >> 23) & 1) == 0;
        if (negative)
        {
            // It looks like other similar clients don't parse TIME2 values properly
            // In negative time values both TIME and FSP are stored in reverse order
            // See https://github.com/mysql/mysql-server/blob/ea7d2e2d16ac03afdd9cb72a972a95981107bf51/sql/log_event.cc#L2022
            // See https://github.com/mysql/mysql-server/blob/ea7d2e2d16ac03afdd9cb72a972a95981107bf51/mysys/my_time.cc#L1784
            throw new NotSupportedException("Parsing negative TIME values is not supported in this version");
        }

        // 1 bit sign. 1 bit unused. 10 bits hour. 6 bits minute. 6 bits second.
        var hour = (value >> 12) % (1 << 10);
        var minute = (value >> 6) % (1 << 6);
        var second = value % (1 << 6);

        return new TimeSpan(0, hour, minute, second, millisecond);
    }

    public DateTimeOffset ParseTimeStamp2(ref PacketReader reader, int metadata)
    {
        long seconds = reader.ReadUInt32BigEndian();
        var millisecond = ParseFractionalPart(ref reader, metadata) / 1000;
        var timestamp = seconds * 1000 + millisecond;

        return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
    }

    public DateTime? ParseDateTime2(ref PacketReader reader, int metadata)
    {
        var value = reader.ReadLongBigEndian(5);
        var millisecond = ParseFractionalPart(ref reader, metadata) / 1000;

        // 1 bit sign(always true). 17 bits year*13+month. 5 bits day. 5 bits hour. 6 bits minute. 6 bits second.
        var yearMonth = (int)((value >> 22) % (1 << 17));
        var year = yearMonth / 13;
        var month = yearMonth % 13;
        var day = (int)((value >> 17) % (1 << 5));
        var hour = (int)((value >> 12) % (1 << 5));
        var minute = (int)((value >> 6) % (1 << 6));
        var second = (int)(value % (1 << 6));

        if (year == 0 || month == 0 || day == 0)
            return null;

        return new DateTime(year, month, day, hour, minute, second, millisecond);
    }

    private int ParseFractionalPart(ref PacketReader reader, int metadata)
    {
        var length = (metadata + 1) / 2;
        if (length == 0)
            return 0;

        var fraction = reader.ReadIntBigEndian(length);
        return fraction * (int)Math.Pow(100, 3 - length);
    }
}