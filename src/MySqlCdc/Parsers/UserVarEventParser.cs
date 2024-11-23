using MySqlCdc.Events;
using MySqlCdc.Protocol;

namespace MySqlCdc.Parsers;

/// <summary>
/// Parses <see cref="UserVarEvent"/> events.
/// Supports all versions of MariaDB and MySQL.
/// </summary>
public class UserVarEventParser : IEventParser
{
    /// <summary>
    /// Parses <see cref="UserVarEvent"/> from the buffer.
    /// </summary>
    public IBinlogEvent ParseEvent(EventHeader header, ref PacketReader reader)
    {
        var nameLength = (int)reader.ReadUInt32LittleEndian();
        var name = reader.ReadString(nameLength);

        var isNull = reader.ReadByte() != 0; // 0 indicates there is a value
        if (isNull)
            return new UserVarEvent(name, null);

        var variableType = reader.ReadByte();
        var collationNumber = (int)reader.ReadUInt32LittleEndian();

        var valueLength = (int)reader.ReadUInt32LittleEndian();
        var value = reader.ReadString(valueLength);

        var flags = reader.ReadByte();

        return new UserVarEvent(name, new VariableValue(variableType, collationNumber, value, flags));
    }
}