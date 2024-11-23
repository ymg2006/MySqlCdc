using MySqlCdc.Protocol;

namespace MySqlCdc.Commands;

/// <summary>
/// Requests binlog event stream.
/// <a href="https://mariadb.com/kb/en/library/com_binlog_dump/">See more</a>
/// </summary>
internal class DumpBinlogCommand(long serverId, string binlogFilename, long binlogPosition, int flags = 0)
    : ICommand
{
    public long ServerId { get; } = serverId;
    public string BinlogFilename { get; } = binlogFilename;
    public long BinlogPosition { get; } = binlogPosition;
    public int Flags { get; } = flags;

    public byte[] Serialize()
    {
        var writer = new PacketWriter();
        writer.WriteByte((byte)CommandType.BinlogDump);
        writer.WriteLongLittleEndian(BinlogPosition, 4);
        writer.WriteIntLittleEndian(Flags, 2);
        writer.WriteLongLittleEndian(ServerId, 4);
        writer.WriteString(BinlogFilename);
        return writer.CreatePacket();
    }
}