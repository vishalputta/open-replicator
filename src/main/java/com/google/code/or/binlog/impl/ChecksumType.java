package com.google.code.or.binlog.impl;

/**
 * @see <a
 *      href="https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#option_mysqld_binlog-checksum">
 *      MySQL --binlog-checksum option </a>
 * @author yogesh.dahiya
 */

public enum ChecksumType
{

	NONE(0), CRC32(4);

	private int length;

	private ChecksumType(int length)
	{
		this.length = length;
	}

	public int getLength()
	{
		return length;
	}

}
