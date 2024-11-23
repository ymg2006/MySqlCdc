using MySqlCdc.Events;
using MySqlCdc.Parsers;
using MySqlCdc.Protocol;

namespace MySqlCdc.Providers.MariaDb;

/// <summary>
/// Parses <see cref="GtidListEvent"/> events in MariaDB.
/// </summary>
public class GtidListEventParser : IEventParser
{
    /// <summary>
    /// Parses <see cref="GtidListEvent"/> from the buffer.
    /// </summary>
    public IBinlogEvent ParseEvent(EventHeader header, ref PacketReader reader)
    {
        var gtidListLength = reader.ReadUInt32LittleEndian();

        var gtidList = new GtidList();
        for (var i = 0; i < gtidListLength; i++)
        {
            long domainId = reader.ReadUInt32LittleEndian();
            long serverId = reader.ReadUInt32LittleEndian();
            var sequence = reader.ReadInt64LittleEndian();

            gtidList.Gtids.Add(new Gtid(domainId, serverId, sequence));
        }

        return new GtidListEvent(gtidList);
    }
}