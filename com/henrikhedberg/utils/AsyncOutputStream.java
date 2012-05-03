/*
 * AsyncOutputStream - Asynchronous Output Stream Filter
 *
 * (c) 2012 Henrik Hedberg <henrik.hedberg@iki.fi>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 *
 */

package com.henrikhedberg.utils;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * This class implements an asynchronous output stream which
 * buffers the written data internally and writes it to the
 * underlying output stream in a separate thread. 
 * 
 * @author Henrik Hedberg
 */

public class AsyncOutputStream extends FilterOutputStream {
	private byte buffer[];
	private int count;
	private boolean closing;
	private IOException pendingException;
	
	/**
	 * Creates a new asynchronous output stream. Starts also
	 * a separate thread to perform the actual write to the
	 * underlying output stream asynchronously.
	 *  
	 * @param out the underlying output stream.
	 */
	public AsyncOutputStream(OutputStream out) {
		super(out);

		new Thread() {
			public void run() {
				try {
					loop();
				} catch (InterruptedException e) {
				}
			}
		}.start();
	}
	
	/**
	 * Schedules the underlying output stream to be closed.
	 * 
	 * <p>Note that the actual operation is performed some
	 * time later in a separate thread.
	 * 
	 * @exception IOException if the stream is already closed.
	 */
	@Override
	public synchronized void close() throws IOException {
		if (closing)
			throw new IOException("Output stream already closed.");

		closing = true;
		notify();
	}

	/**
	 * Schedules the underlying output stream to be flushed.
	 * 
	 * <p>Note that the actual operation is performed some
	 * time later in a separate thread.
	 * 
	 * @exception IOException if there was a pending exception.
	 */
	@Override
	public synchronized void flush() throws IOException {
		ensureState();

		notify();
	}

	/**
	 * Schedules the <code>length</code> bytes from the specified
	 * byte array <code>bytes</code> starting at offset <code>offset</code>
	 * to be written into the underlying output stream.
	 * 
	 * <p>Note that the actual operation is performed some
	 * time later in a separate thread.
	 * 
	 * @param bytes the data.
	 * @param offset the start offset in the data.
	 * @param length the number of bytes to write.
	 * @exception IOException if there was a pending exception.

	 */
	@Override
	public synchronized void write(byte[] bytes, int offset, int length) throws IOException {
		ensureState();

		growIfNeeded(length);
		System.arraycopy(bytes, offset, buffer, count, length);
		count += length;
		notify();
	}
	
	/**
	 * Schedules the specified byte <code>oneByte</code> to be
	 * written into the underlying output stream.
	 * 
	 * <p>Note that the actual operation is performed some
	 * time later in a separate thread.
	 * 
	 * @exception IOException if there was a pending exception.

	 */
	@Override
	public synchronized void write(int oneByte) throws IOException {
		ensureState();
		
		growIfNeeded(1);
		buffer[count] = (byte)oneByte;
		count += 1;
		notify();
	}
	
	private void ensureState() throws IOException {
		if (pendingException != null) {
			IOException exception = pendingException;
			pendingException = null;
			throw exception;
		}
		if (closing)
			throw new IOException("Output stream already closed.");	
	}
	
	private void growIfNeeded(int length) {
		if (buffer == null)
			buffer = new byte[length];
		else if (buffer.length < count + length)
			buffer = Arrays.copyOf(buffer, count + length);			
	}
	
	private void loop() throws InterruptedException {
		byte[] bytes = null;
		int length = 0;
		boolean looping = true;

		while (looping) {
			synchronized (this) {
				if (buffer == null)
					buffer = bytes;
				bytes = null;
				
				if (count == 0 && !closing)
					wait();

				if (count > 0) {
					bytes = buffer;
					length = count;
					buffer = null;
					count = 0;
				}
				if (closing)
					looping = false;
			}

			try {
				if (bytes != null) {
					out.write(bytes, 0, length);
					if (!looping)
						out.flush();
				} else {
					out.flush();
				}
			} catch (IOException exception) {
				if (pendingException == null)
					pendingException = exception;
			}
		}
		
		try {
			out.close();
		} catch (IOException exception) {
			if (pendingException == null)
				pendingException = exception;
		}
	}
}
