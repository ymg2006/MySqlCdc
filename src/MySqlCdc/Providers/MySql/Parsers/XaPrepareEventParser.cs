using MySqlCdc.Events;
using MySqlCdc.Parsers;
using MySqlCdc.Protocol;

namespace MySqlCdc.Providers.MySql;

/// <summary>
/// Parses <see cref="XaPrepareEvent"/> events in MySQL 5.6+.
/// </summary>
public class XaPrepareEventParser : IEventParser
{
    /// <summary>
    /// Parses <see cref="XaPrepareEvent"/> from the buffer.
    /// </summary>
    public IBinlogEvent ParseEvent(EventHeader header, ref PacketReader reader)
    {
        var onePhase = reader.ReadByte() != 0x00;
        var formatId = (int)reader.ReadUInt32LittleEndian();
        var gtridLength = (int)reader.ReadUInt32LittleEndian();
        var bqualLength = (int)reader.ReadUInt32LittleEndian();
        var gtrid = reader.ReadString(gtridLength);
        var bqual = reader.ReadString(bqualLength);

        return new XaPrepareEvent(onePhase, formatId, gtrid, bqual);
    }
}