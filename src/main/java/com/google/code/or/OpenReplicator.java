/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.code.or;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.BinlogParserListener;
import com.google.code.or.binlog.impl.ChecksumType;
import com.google.code.or.binlog.impl.ReplicationBasedBinlogParser;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventParser;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.FormatDescriptionEventParser;
import com.google.code.or.binlog.impl.parser.IncidentEventParser;
import com.google.code.or.binlog.impl.parser.IntvarEventParser;
import com.google.code.or.binlog.impl.parser.QueryEventParser;
import com.google.code.or.binlog.impl.parser.RandEventParser;
import com.google.code.or.binlog.impl.parser.RotateEventParser;
import com.google.code.or.binlog.impl.parser.StopEventParser;
import com.google.code.or.binlog.impl.parser.TableMapEventParser;
import com.google.code.or.binlog.impl.parser.UpdateRowsEventParser;
import com.google.code.or.binlog.impl.parser.UpdateRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.UserVarEventParser;
import com.google.code.or.binlog.impl.parser.WriteRowsEventParser;
import com.google.code.or.binlog.impl.parser.WriteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.XidEventParser;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.util.BackoffTimer;
import com.google.code.or.common.util.BackoffTimer.BackoffTimerConfig;
import com.google.code.or.common.util.QueryUtil;
import com.google.code.or.io.impl.SocketFactoryImpl;
import com.google.code.or.net.Packet;
import com.google.code.or.net.Transport;
import com.google.code.or.net.TransportException;
import com.google.code.or.net.impl.AuthenticatorImpl;
import com.google.code.or.net.impl.TransportImpl;
import com.google.code.or.net.impl.packet.ErrorPacket;
import com.google.code.or.net.impl.packet.ResultSetRowPacket;
import com.google.code.or.net.impl.packet.command.ComBinlogDumpPacket;

/**
 * @author Jingqi Xu
 */
public class OpenReplicator
{
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryUtil.class);
	//
	protected int port = 3306;
	protected String host;
	protected String user;
	protected String password;
	protected int serverId = 6789;
	protected String binlogFileName;
	protected long binlogPosition = 4;
	protected String encoding = "utf-8";
	protected int level1BufferSize = 1024 * 1024;
	protected int level2BufferSize = 8 * 1024 * 1024;
	protected int socketReceiveBufferSize = 512 * 1024;

	//
	protected Transport transport;
	protected BinlogParser binlogParser;
	protected BinlogEventListener binlogEventListener;
	protected final AtomicBoolean running = new AtomicBoolean(false);
	protected BackoffTimer retryCounter = new BackoffTimer(new BackoffTimerConfig(1, 60000, 2, 5, 20), "parserRetry");

	private class ORBinlogParserListener implements BinlogParserListener
	{

		public void onStart(BinlogParser parser)
		{

		}

		public void onStop(BinlogParser parser)
		{
			stopQuietly(0, TimeUnit.MILLISECONDS);

		}

		public void onException(BinlogParser parser, Exception exception)
		{
			LOGGER.error("Exception occured in binlogParser", exception);
			LOGGER.info("Retrying the prassing from : " + parser.getContext().getBinlogFileName() + ":"
			        + parser.getContext().getCurrentPosition());
			retry(parser.getContext());
		}

	}

	/**
	 * 
	 */
	public boolean isRunning()
	{
		return this.running.get();
	}

	public void start() throws Exception
	{
		//
		if (!this.running.compareAndSet(false, true))
		{
			return;
		}

		//
		if (this.transport == null)
			this.transport = getDefaultTransport();
		this.transport.connect(this.host, this.port);

		if (this.binlogParser == null)
			this.binlogParser = getDefaultBinlogParser();
		this.binlogParser.setEventListener(this.binlogEventListener);
		this.binlogParser.setChecksumLength(fetchBinlogChecksum(this.transport).getLength());
		this.binlogParser.addParserListener(new ORBinlogParserListener());

		dumpBinLog(this.binlogFileName, this.binlogPosition);
		this.binlogParser.start();
	}

	protected void dumpBinLog(String binlogFileName, long binlogPosition) throws Exception
	{
		//
		final ComBinlogDumpPacket command = new ComBinlogDumpPacket();
		command.setBinlogFlag(0);
		command.setServerId(this.serverId);
		command.setBinlogPosition(binlogPosition);
		command.setBinlogFileName(StringColumn.valueOf(binlogFileName.getBytes(this.encoding)));
		this.transport.getOutputStream().writePacket(command);
		this.transport.getOutputStream().flush();

		//
		final Packet packet = this.transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER)
		{
			final ErrorPacket error = ErrorPacket.valueOf(packet);
			throw new TransportException(error);
		}

	}

	public ChecksumType fetchBinlogChecksum(Transport transport) throws IOException
	{
		// #https://dev.mysql.com/worklog/task/?id=2540
		List<ResultSetRowPacket> resultSet = QueryUtil.query("show global variables like 'binlog_checksum'", transport);
		ChecksumType checksumType =
		        resultSet.size() > 0 ? ChecksumType.valueOf(resultSet.get(0).getColumns().get(1).toString()
		                .toUpperCase()) : ChecksumType.NONE;

		// checksum handshake
		if (checksumType != ChecksumType.NONE)
		{
			QueryUtil.query("set @master_binlog_checksum= @@global.binlog_checksum", transport);
		}
		return checksumType;
	}

	public void stop(long timeout, TimeUnit unit) throws Exception
	{
		//
		if (!this.running.compareAndSet(true, false))
		{
			return;
		}

		//
		this.transport.disconnect();
		this.binlogParser.stop(timeout, unit);
	}

	public void stopQuietly(long timeout, TimeUnit unit)
	{
		try
		{
			stop(timeout, unit);
		}
		catch (Exception e)
		{
			// NOP
		}
	}

	public void retry(BinlogParserContext context)
	{
		try
		{
			stopQuietly(0, TimeUnit.MILLISECONDS);
			if (retryCounter.backoff() < 0)
				throw new Exception("No success with retry");
			retryCounter.sleep();

			this.transport = getDefaultTransport();
			this.transport.connect(this.host, this.port);
			this.binlogParser = getDefaultBinlogParser();
			/** setting the current context */
			this.binlogParser.setContext(context);
			this.binlogParser.setEventListener(this.binlogEventListener);
			this.binlogParser.setChecksumLength(fetchBinlogChecksum(this.transport).getLength());
			this.binlogParser.addParserListener(new ORBinlogParserListener());

			dumpBinLog(context.getBinlogFileName(), context.getCurrentPosition());
			this.binlogParser.start();
		}
		catch (Exception ex)
		{
			LOGGER.error("Failed to retry", ex);
		}

	}

	/**
	 * 
	 */
	public int getPort()
	{
		return port;
	}

	public void setPort(int port)
	{
		this.port = port;
	}

	public String getHost()
	{
		return host;
	}

	public void setHost(String host)
	{
		this.host = host;
	}

	public String getUser()
	{
		return user;
	}

	public void setUser(String user)
	{
		this.user = user;
	}

	public String getPassword()
	{
		return password;
	}

	public void setPassword(String password)
	{
		this.password = password;
	}

	public String getEncoding()
	{
		return encoding;
	}

	public void setEncoding(String encoding)
	{
		this.encoding = encoding;
	}

	public int getServerId()
	{
		return serverId;
	}

	public void setServerId(int serverId)
	{
		this.serverId = serverId;
	}

	public long getBinlogPosition()
	{
		return binlogPosition;
	}

	public void setBinlogPosition(long binlogPosition)
	{
		this.binlogPosition = binlogPosition;
	}

	public String getBinlogFileName()
	{
		return binlogFileName;
	}

	public void setBinlogFileName(String binlogFileName)
	{
		this.binlogFileName = binlogFileName;
	}

	public int getLevel1BufferSize()
	{
		return level1BufferSize;
	}

	public void setLevel1BufferSize(int level1BufferSize)
	{
		this.level1BufferSize = level1BufferSize;
	}

	public int getLevel2BufferSize()
	{
		return level2BufferSize;
	}

	public void setLevel2BufferSize(int level2BufferSize)
	{
		this.level2BufferSize = level2BufferSize;
	}

	public int getSocketReceiveBufferSize()
	{
		return socketReceiveBufferSize;
	}

	public void setSocketReceiveBufferSize(int socketReceiveBufferSize)
	{
		this.socketReceiveBufferSize = socketReceiveBufferSize;
	}

	/**
	 * 
	 */
	public Transport getTransport()
	{
		return transport;
	}

	public void setTransport(Transport transport)
	{
		this.transport = transport;
	}

	public BinlogParser getBinlogParser()
	{
		return binlogParser;
	}

	public void setBinlogParser(BinlogParser parser)
	{
		this.binlogParser = parser;
	}

	public BinlogEventListener getBinlogEventListener()
	{
		return binlogEventListener;
	}

	public void setBinlogEventListener(BinlogEventListener listener)
	{
		this.binlogEventListener = listener;
	}

	protected Transport getDefaultTransport() throws Exception
	{
		//
		final TransportImpl r = new TransportImpl();
		r.setLevel1BufferSize(this.level1BufferSize);
		r.setLevel2BufferSize(this.level2BufferSize);

		//
		final AuthenticatorImpl authenticator = new AuthenticatorImpl();
		authenticator.setUser(this.user);
		authenticator.setPassword(this.password);
		authenticator.setEncoding(this.encoding);
		r.setAuthenticator(authenticator);

		//
		final SocketFactoryImpl socketFactory = new SocketFactoryImpl();
		socketFactory.setKeepAlive(true);
		socketFactory.setTcpNoDelay(false);
		socketFactory.setReceiveBufferSize(this.socketReceiveBufferSize);
		r.setSocketFactory(socketFactory);
		return r;
	}

	protected ReplicationBasedBinlogParser getDefaultBinlogParser() throws Exception
	{
		//
		final ReplicationBasedBinlogParser r =
		        new ReplicationBasedBinlogParser(this.binlogFileName, this.binlogPosition);
		r.registgerEventParser(new StopEventParser());
		r.registgerEventParser(new RotateEventParser());
		r.registgerEventParser(new IntvarEventParser());
		r.registgerEventParser(new XidEventParser());
		r.registgerEventParser(new RandEventParser());
		r.registgerEventParser(new QueryEventParser());
		r.registgerEventParser(new UserVarEventParser());
		r.registgerEventParser(new IncidentEventParser());
		r.registgerEventParser(new TableMapEventParser());
		r.registgerEventParser(new WriteRowsEventParser());
		r.registgerEventParser(new UpdateRowsEventParser());
		r.registgerEventParser(new DeleteRowsEventParser());
		r.registgerEventParser(new WriteRowsEventV2Parser());
		r.registgerEventParser(new UpdateRowsEventV2Parser());
		r.registgerEventParser(new DeleteRowsEventV2Parser());
		r.registgerEventParser(new FormatDescriptionEventParser());

		//
		r.setTransport(this.transport);
		return r;
	}
}
