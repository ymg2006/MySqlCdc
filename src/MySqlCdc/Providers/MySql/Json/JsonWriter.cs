using System.Text.Json;
using MySqlCdc.Constants;

namespace MySqlCdc.Providers.MySql;

internal class JsonWriter(Utf8JsonWriter writer) : IJsonWriter
{
    private string? _propertyName;

    public void WriteKey(string name)
    {
        _propertyName = name;
    }

    public void WriteStartObject()
    {
        if (_propertyName != null)
        {
            writer.WriteStartObject(_propertyName);
            _propertyName = null;
        }
        else writer.WriteStartObject();
    }

    public void WriteStartArray()
    {
        if (_propertyName != null)
        {
            writer.WriteStartArray(_propertyName);
            _propertyName = null;
        }
        else writer.WriteStartArray();
    }

    public void WriteEndObject()
    {
        writer.WriteEndObject();
    }

    public void WriteEndArray()
    {
        writer.WriteEndArray();
    }

    public void WriteValue(short value)
    {
        if (_propertyName != null)
        {
            writer.WriteNumber(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteNumberValue(value);
    }

    public void WriteValue(ushort value)
    {
        if (_propertyName != null)
        {
            writer.WriteNumber(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteNumberValue(value);
    }

    public void WriteValue(int value)
    {
        if (_propertyName != null)
        {
            writer.WriteNumber(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteNumberValue(value);
    }

    public void WriteValue(uint value)
    {
        if (_propertyName != null)
        {
            writer.WriteNumber(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteNumberValue(value);
    }

    public void WriteValue(long value)
    {
        if (_propertyName != null)
        {
            writer.WriteNumber(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteNumberValue(value);
    }

    public void WriteValue(ulong value)
    {
        if (_propertyName != null)
        {
            writer.WriteNumber(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteNumberValue(value);
    }

    public void WriteValue(double value)
    {
        if (_propertyName != null)
        {
            writer.WriteNumber(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteNumberValue(value);
    }

    public void WriteValue(string value)
    {
        if (_propertyName != null)
        {
            writer.WriteString(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteStringValue(value);
    }

    public void WriteValue(bool value)
    {
        if (_propertyName != null)
        {
            writer.WriteBoolean(_propertyName, value);
            _propertyName = null;
        }
        else writer.WriteBooleanValue(value);
    }

    public void WriteNull()
    {
        if (_propertyName != null)
        {
            writer.WriteNull(_propertyName);
            _propertyName = null;
        }
        else writer.WriteNullValue();
    }

    public void WriteDate(DateTime value)
    {
        WriteValue(value.ToString("yyyy-MM-dd"));
    }

    public void WriteTime(TimeSpan value)
    {
        WriteValue(value.ToString("hh':'mm':'ss'.'fff"));
    }

    public void WriteDateTime(DateTime value)
    {
        WriteValue(value.ToString("yyyy-MM-dd HH:mm:ss.fff"));
    }

    public void WriteOpaque(ColumnType columnType, byte[] value)
    {
        WriteValue(Convert.ToBase64String(value));
    }
}