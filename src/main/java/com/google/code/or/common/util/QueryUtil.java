package com.google.code.or.common.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.common.glossary.UnsignedLong;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.net.Packet;
import com.google.code.or.net.Transport;
import com.google.code.or.net.TransportException;
import com.google.code.or.net.impl.packet.EOFPacket;
import com.google.code.or.net.impl.packet.ErrorPacket;
import com.google.code.or.net.impl.packet.ResultSetFieldPacket;
import com.google.code.or.net.impl.packet.ResultSetHeaderPacket;
import com.google.code.or.net.impl.packet.ResultSetRowPacket;
import com.google.code.or.net.impl.packet.command.ComQuery;

/**
 * @see http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
 * @author yogesh.dahiya
 */

public class QueryUtil
{
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryUtil.class);

	public static List<ResultSetRowPacket> query(String sql, Transport transport) throws IOException
	{
		List<ResultSetRowPacket> resultSet = new LinkedList<ResultSetRowPacket>();

		final ComQuery command = new ComQuery();
		command.setSql(StringColumn.valueOf(sql.getBytes()));
		transport.getOutputStream().writePacket(command);
		transport.getOutputStream().flush();

		Packet packet = transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER)
		{
			final ErrorPacket error = ErrorPacket.valueOf(packet);
			LOGGER.info("{}", error);
			throw new TransportException(error);
		}

		final ResultSetHeaderPacket header = ResultSetHeaderPacket.valueOf(packet);
		LOGGER.info("{}", header);

		if (header.getFieldCount() == UnsignedLong.valueOf(0))
			return resultSet;

		while (true)
		{
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER)
			{
				EOFPacket eof = EOFPacket.valueOf(packet);
				LOGGER.info("{}", eof);
				break;
			}
			else
			{
				ResultSetFieldPacket field = ResultSetFieldPacket.valueOf(packet);
				LOGGER.info("{}", field);
			}
		}

		while (true)
		{
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER)
			{
				EOFPacket eof = EOFPacket.valueOf(packet);
				LOGGER.info("{}", eof);
				break;
			}
			else
			{
				ResultSetRowPacket row = ResultSetRowPacket.valueOf(packet);
				resultSet.add(row);
				LOGGER.info("{}", row);
			}
		}
		return resultSet;
	}

}
