using MySqlCdc.Protocol;
using MySqlCdc.Providers.MySql;

namespace MySqlCdc.Commands;

/// <summary>
/// Requests binlog event stream by GtidSet.
/// <a href="https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html">See more</a>
/// </summary>
internal class DumpBinlogGtidCommand(
    long serverId,
    string binlogFilename,
    long binlogPosition,
    GtidSet gtidSet,
    int flags = 0)
    : ICommand
{
    public long ServerId { get; } = serverId;
    public string BinlogFilename { get; } = binlogFilename;
    public long BinlogPosition { get; } = binlogPosition;
    public GtidSet GtidSet { get; } = gtidSet;
    public int Flags { get; } = flags;

    public byte[] Serialize()
    {
        var writer = new PacketWriter();

        writer.WriteByte((byte)CommandType.BinlogDumpGtid);
        writer.WriteIntLittleEndian(Flags, 2);
        writer.WriteLongLittleEndian(ServerId, 4);

        writer.WriteIntLittleEndian(BinlogFilename.Length, 4);
        writer.WriteString(BinlogFilename);
        writer.WriteLongLittleEndian(BinlogPosition, 8);

        var dataLength = 8; /* Number of UuidSets */
        foreach (var uuidSet in GtidSet.UuidSets.Values)
        {
            dataLength += 16;   /* SourceId */
            dataLength += 8;    /* Number of intervals */
            dataLength += uuidSet.Intervals.Count * (8 + 8) /* Start-End */;
        }

        writer.WriteIntLittleEndian(dataLength, 4);
        writer.WriteLongLittleEndian(GtidSet.UuidSets.Count, 8);

        foreach (var uuidSet in GtidSet.UuidSets.Values)
        {
            writer.WriteByteArray(uuidSet.SourceId.ToByteArray());
            writer.WriteLongLittleEndian(uuidSet.Intervals.Count, 8);
            foreach (var interval in uuidSet.Intervals)
            {
                writer.WriteLongLittleEndian(interval.Start, 8);
                writer.WriteLongLittleEndian(interval.End + 1, 8);
            }
        }
        return writer.CreatePacket();
    }
}