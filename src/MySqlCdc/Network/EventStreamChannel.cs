using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using MySqlCdc.Constants;
using MySqlCdc.Protocol;

namespace MySqlCdc.Network;

internal class EventStreamChannel(IEventStreamReader eventStreamReader, Stream stream)
{
    private readonly PipeReader _pipeReader = PipeReader.Create(stream);
    private List<PacketSegment>? _multipacket;

    public async IAsyncEnumerable<IPacket> ReadPacketAsync(TimeSpan timeout, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await _pipeReader.ReadAsync(cancellationToken)
                .AsTask()
                .WithTimeout(timeout, TimeoutConstants.Message);

            var buffer = result.Buffer;

            while (!cancellationToken.IsCancellationRequested)
            {
                // We can't calculate packet size without the packet header
                if (buffer.Length < PacketConstants.HeaderSize)
                    break;

                // Make sure the packet fits in the buffer
                // See: https://mariadb.com/kb/en/library/0-packet/
                var bodySize = GetBodySize(buffer);
                var packetSize = PacketConstants.HeaderSize + bodySize;

                if (buffer.Length < packetSize)
                    break;

                // Process packet and repeat in case there are more packets in the buffer
                var packet = TryReadPacket(buffer.Slice(PacketConstants.HeaderSize, bodySize));

                if (packet != null)
                    yield return packet;

                buffer = buffer.Slice(buffer.GetPosition(packetSize));
            }

            _pipeReader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted)
                break;
        }

        await _pipeReader.CompleteAsync();
    }

    private int GetBodySize(ReadOnlySequence<byte> buffer)
    {
        Span<byte> span = stackalloc byte[PacketConstants.HeaderSize];
        buffer.Slice(0, PacketConstants.HeaderSize).CopyTo(span);
        var reader = new PacketReader(span);
        var bodySize = reader.ReadIntLittleEndian(3);
        return bodySize;
    }

    private IPacket? TryReadPacket(ReadOnlySequence<byte> buffer)
    {
        if (_multipacket != null || buffer.Length == PacketConstants.MaxBodyLength)
        {
            var array = new byte[buffer.Length];
            buffer.CopyTo(array);

            if (_multipacket == null)
            {
                _multipacket = new List<PacketSegment> { new(array) };
            }
            else
            {
                var lastNode = _multipacket.Last();
                _multipacket.Add(lastNode.Add(array));
            }

            if (buffer.Length == PacketConstants.MaxBodyLength)
                return null;

            var firstSegment = _multipacket.First();
            var lastSegment = _multipacket.Last();
            buffer = new ReadOnlySequence<byte>(firstSegment, 0, lastSegment, lastSegment.Memory.Length);
            _multipacket = null;
        }

        return eventStreamReader.ReadPacket(buffer);
    }
}