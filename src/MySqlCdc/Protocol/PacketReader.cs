using System.Buffers.Binary;
using System.Text;
using MySqlCdc.Constants;

namespace MySqlCdc.Protocol;

/// <summary>
/// Constructs server reply from byte packet response.
/// </summary>
public ref struct PacketReader
{
    private ReadOnlySpan<byte> _span;
    private int _offset;

    /// <summary>
    /// Creates a new <see cref="PacketReader"/>.
    /// </summary>
    public PacketReader(ReadOnlySpan<byte> span)
    {
        _span = span;
        _offset = 0;
    }

    /// <summary>
    /// Reads one byte as int number.
    /// </summary>
    public byte ReadByte() => _span[_offset++];

    /// <summary>
    /// Reads 16-bit int written in little-endian format.
    /// </summary>
    public UInt16 ReadUInt16LittleEndian()
    {
        var result = BinaryPrimitives.ReadUInt16LittleEndian(_span.Slice(_offset));
        _offset += 2;
        return result;
    }

    /// <summary>
    /// Reads 16-bit int written in big-endian format.
    /// </summary>
    public UInt16 ReadUInt16BigEndian()
    {
        var result = BinaryPrimitives.ReadUInt16BigEndian(_span.Slice(_offset));
        _offset += 2;
        return result;
    }

    /// <summary>
    /// Reads 32-bit int written in little-endian format.
    /// </summary>
    public UInt32 ReadUInt32LittleEndian()
    {
        var result = BinaryPrimitives.ReadUInt32LittleEndian(_span.Slice(_offset));
        _offset += 4;
        return result;
    }

    /// <summary>
    /// Reads 32-bit int written in big-endian format.
    /// </summary>
    public UInt32 ReadUInt32BigEndian()
    {
        var result = BinaryPrimitives.ReadUInt32BigEndian(_span.Slice(_offset));
        _offset += 4;
        return result;
    }

    /// <summary>
    /// Reads 64-bit long written in little-endian format.
    /// </summary>
    public UInt64 ReadUInt64LittleEndian()
    {
        var result = BinaryPrimitives.ReadUInt64LittleEndian(_span.Slice(_offset));
        _offset += 8;
        return result;
    }

    /// <summary>
    /// Reads 64-bit long written in little-endian format.
    /// </summary>
    public long ReadInt64LittleEndian()
    {
        var result = BinaryPrimitives.ReadInt64LittleEndian(_span.Slice(_offset));
        _offset += 8;
        return result;
    }

    /// <summary>
    /// Reads int number written in little-endian format.
    /// </summary>
    public int ReadIntLittleEndian(int length)
    {
        var result = 0;
        for (var i = 0; i < length; i++)
        {
            result |= _span[_offset + i] << (i << 3);
        }
        _offset += length;
        return result;
    }

    /// <summary>
    /// Reads long number written in little-endian format.
    /// </summary>
    public long ReadLongLittleEndian(int length)
    {
        long result = 0;
        for (var i = 0; i < length; i++)
        {
            result |= (long)_span[_offset + i] << (i << 3);
        }
        _offset += length;
        return result;
    }

    /// <summary>
    /// Reads int number written in big-endian format.
    /// </summary>
    public int ReadIntBigEndian(int length)
    {
        var result = 0;
        for (var i = 0; i < length; i++)
        {
            result = (result << 8) | _span[_offset + i];
        }
        _offset += length;
        return result;
    }

    /// <summary>
    /// Reads long number written in big-endian format.
    /// </summary>
    public long ReadLongBigEndian(int length)
    {
        long result = 0;
        for (var i = 0; i < length; i++)
        {
            result = (result << 8) | _span[_offset + i];
        }
        _offset += length;
        return result;
    }

    /// <summary>
    /// if first byte is less than 0xFB - Integer value is this 1 byte integer
    /// 0xFB - NULL value
    /// 0xFC - Integer value is encoded in the next 2 bytes (3 bytes total)
    /// 0xFD - Integer value is encoded in the next 3 bytes (4 bytes total)
    /// 0xFE - Integer value is encoded in the next 8 bytes (9 bytes total)
    /// </summary>
    public int ReadLengthEncodedNumber()
    {
        var firstByte = ReadByte();

        if (firstByte < 0xFB)
            return firstByte;
        if (firstByte == 0xFB)
            throw new FormatException("Length encoded integer cannot be NULL.");
        if (firstByte == 0xFC)
            return ReadUInt16LittleEndian();
        if (firstByte == 0xFD)
            return ReadIntLittleEndian(3);
        if (firstByte == 0xFE)
        {
            var value = ReadInt64LittleEndian();
            if (value < 0 || value > Int32.MaxValue)
                throw new OverflowException($"Length encoded integer cannot exceed {nameof(Int32.MaxValue)}.");

            // Max theoretical length of .NET strings, arrays is Int32.MaxValue
            return (int)value;
        }
        throw new FormatException($"Unexpected length-encoded integer: {firstByte}");
    }

    /// <summary>
    /// Reads fixed length string.
    /// </summary>
    public string ReadString(int length)
    {
        var span = _span.Slice(_offset, length);
        _offset += length;
        return ParseString(span);
    }

    /// <summary>
    /// Reads string to end of the sequence.
    /// </summary>
    public string ReadStringToEndOfFile()
    {
        var span = _span.Slice(_offset);
        _offset += span.Length;
        return ParseString(span);
    }

    /// <summary>
    /// Reads string terminated by 0 byte.
    /// </summary>
    public string ReadNullTerminatedString()
    {
        var index = 0;
        while (true)
        {
            if (_span[_offset + index++] == PacketConstants.NullTerminator)
                break;
        }
        var span = _span.Slice(_offset, index - 1);
        _offset += index;
        return ParseString(span);
    }

    /// <summary>
    /// Reads length-encoded string.
    /// </summary>
    public string ReadLengthEncodedString()
    {
        var length = ReadLengthEncodedNumber();
        return ReadString(length);
    }

    /// <summary>
    /// Reads byte array from the sequence.
    /// Allocates managed memory for the array.
    /// </summary>
    public byte[] ReadByteArraySlow(int length)
    {
        var span = _span.Slice(_offset, length);
        _offset += length;
        return span.ToArray();
    }

    /// <summary>
    /// Reads bitmap in little-endian bytes order
    /// </summary>
    public bool[] ReadBitmapLittleEndian(int bitsNumber)
    {
        var result = new bool[bitsNumber];
        var bytesNumber = (bitsNumber + 7) / 8;
        for (var i = 0; i < bytesNumber; i++)
        {
            var value = _span[_offset + i];
            for (var y = 0; y < 8; y++)
            {
                var index = (i << 3) + y;
                if (index == bitsNumber)
                    break;
                result[index] = (value & (1 << y)) > 0;
            }
        }
        _offset += bytesNumber;
        return result;
    }

    /// <summary>
    /// Reads bitmap in big-endian bytes order
    /// </summary>
    public bool[] ReadBitmapBigEndian(int bitsNumber)
    {
        var result = new bool[bitsNumber];
        var bytesNumber = (bitsNumber + 7) / 8;
        for (var i = 0; i < bytesNumber; i++)
        {
            var value = _span[_offset + i];
            for (var y = 0; y < 8; y++)
            {
                var index = ((bytesNumber - i - 1) << 3) + y;
                if (index >= bitsNumber)
                    continue;
                result[index] = (value & (1 << y)) > 0;
            }
        }
        _offset += bytesNumber;
        return result;
    }

    /// <summary>
    /// Checks whether the remaining buffer is empty
    /// </summary>
    public bool IsEmpty() => _span.Length == _offset;

    /// <summary>
    /// Gets number of consumed bytes
    /// </summary>
    public int Consumed => _offset;

    /// <summary>
    /// Skips the specified number of bytes in the buffer
    /// </summary>
    public void Advance(int offset) => _offset += offset;

    /// <summary>
    /// Removes the specified slice from the end
    /// </summary>
    public void SliceFromEnd(int length)
    {
        _span = _span.Slice(0, _span.Length - length);
    }

    /// <summary>
    /// Parses a string from the span.
    /// </summary>
    private string ParseString(ReadOnlySpan<byte> span)
    {
        if (span.Length == 0)
            return string.Empty;

        return Encoding.UTF8.GetString(span);
    }
}