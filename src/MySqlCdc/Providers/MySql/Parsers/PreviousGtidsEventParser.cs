using MySqlCdc.Events;
using MySqlCdc.Parsers;
using MySqlCdc.Protocol;

namespace MySqlCdc.Providers.MySql;

/// <summary>
/// Parses <see cref="PreviousGtidsEvent"/> events in MySQL 5.6+.
/// </summary>
public class PreviousGtidsEventParser : IEventParser
{
    /// <summary>
    /// Parses <see cref="PreviousGtidsEvent"/> from the buffer.
    /// </summary>
    public IBinlogEvent ParseEvent(EventHeader header, ref PacketReader reader)
    {
        var uuidSetNumber = reader.ReadInt64LittleEndian();
        var gtidSet = new GtidSet();

        for (long i = 0; i < uuidSetNumber; i++)
        {
            var sourceId = new Uuid(reader.ReadByteArraySlow(16));
            var uuidSet = new UuidSet(sourceId, new List<Interval>());

            var intervalNumber = reader.ReadInt64LittleEndian();
            for (long y = 0; y < intervalNumber; y++)
            {
                var start = reader.ReadInt64LittleEndian();
                var end = reader.ReadInt64LittleEndian();
                uuidSet.Intervals.Add(new Interval(start, end - 1));
            }
            gtidSet.UuidSets[sourceId] = uuidSet;
        }

        return new PreviousGtidsEvent(gtidSet);
    }
}