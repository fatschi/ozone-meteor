package eu.stratosphere.sopremo.io;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.collect.Lists;

import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.runtime.fs.LineReader;
import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;
import eu.stratosphere.util.CollectionUtil;
import eu.stratosphere.util.Equaler;

@Name(noun = { "csv", "tsv" })
public class CsvFormat extends SopremoFormat {

	/**
	 * The default number of sample lines to consider when calculating the line width.
	 */
	public static final int DEFAULT_NUM_SAMPLES = 10;

	public static final char AUTO = 0;

	private char fieldDelimiter = AUTO;

	private Boolean quotation, header;

	private List<String> keyNames = new ArrayList<String>();

	private List<Class<? extends IJsonNode>> types = new ArrayList<Class<? extends IJsonNode>>();

	private int numLineSamples = DEFAULT_NUM_SAMPLES;

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.io.SopremoFormat#configureForInput(eu.stratosphere.configuration.Configuration,
	 * eu.stratosphere.api.record.operators .GenericDataSource, java.lang.String)
	 */
	@Override
	public void configureForInput(final Configuration configuration, final GenericDataSource<?> source,
			final String inputPath) {
		this.configureDelimiter(inputPath);
		super.configureForInput(configuration, source, inputPath);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.io.SopremoFormat#configureForOutput(eu.stratosphere.configuration.Configuration,
	 * java.net.URI)
	 */
	@Override
	public void configureForOutput(final Configuration configuration, final String outputPath) {
		this.configureDelimiter(outputPath);
		super.configureForOutput(configuration, outputPath);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final CsvFormat other = (CsvFormat) obj;
		return this.fieldDelimiter == other.fieldDelimiter
			&& this.numLineSamples == other.numLineSamples
			&& Equaler.SafeEquals.equal(this.quotation, other.quotation)
			&& Equaler.SafeEquals.equal(this.header, other.header)
			&& this.keyNames.equals(other.keyNames)
			&& this.types.equals(other.types);
	}

	/**
	 * Returns the fieldDelimiter.
	 * 
	 * @return the fieldDelimiter
	 */
	public String getFieldDelimiter() {
		return String.valueOf(this.fieldDelimiter);
	}

	/**
	 * Returns the keyNames.
	 * 
	 * @return the keyNames
	 */
	public List<String> getKeyNames() {
		return this.keyNames;
	}

	/**
	 * Returns the numLineSamples.
	 * 
	 * @return the numLineSamples
	 */
	public int getNumLineSamples() {
		return this.numLineSamples;
	}

	/**
	 * Returns the quotation.
	 * 
	 * @return the quotation
	 */
	public Boolean getQuotation() {
		return this.quotation;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.fieldDelimiter;
		result = prime * result + this.keyNames.hashCode();
		result = prime * result + this.types.hashCode();
		result = prime * result + this.numLineSamples;
		result = prime * result + (this.quotation == null ? 0 : this.quotation.hashCode());
		result = prime * result + (this.header == null ? 0 : this.header.hashCode());
		return result;
	}

	/**
	 * Sets the fieldDelimiter to the specified value.
	 * 
	 * @param fieldDelimiter
	 *        the fieldDelimiter to set
	 */
	@Property
	@Name(noun = "delimiter")
	public void setFieldDelimiter(final String fieldDelimiter) {
		if (fieldDelimiter.length() != 1)
			throw new IllegalArgumentException("field delimiter needs to be exactly one character");
		this.fieldDelimiter = fieldDelimiter.charAt(0);
	}

	/**
	 * Sets the keyNames to the specified value.
	 * 
	 * @param keyNames
	 *        the keyNames to set
	 */
	@Property
	@Name(noun = "columns")
	public void setKeyNames(final List<String> keyNames) {
		if (keyNames == null)
			throw new NullPointerException("keyNames must not be null");

		this.keyNames = new ArrayList<String>(keyNames);
	}

	/**
	 * Sets the keyNames to the specified value.
	 * 
	 * @param keyNames
	 *        the keyNames to set
	 */
	public void setKeyNames(final String... keyNames) {
		setKeyNames(Arrays.asList(keyNames));
	}

	/**
	 * Sets the types to the specified value.
	 * 
	 * @param types
	 *        the types to set
	 */
	@Property
	@Name(noun = "types")
	public void setTypes(List<Class<? extends IJsonNode>> types) {
		if (types == null)
			throw new NullPointerException("types must not be null");

		this.types = types;
	}

	/**
	 * Sets the types to the specified value.
	 * 
	 * @param types
	 *        the types to set
	 */
	public CsvFormat withTypes(List<Class<? extends IJsonNode>> types) {
		setTypes(types);
		return this;
	}

	/**
	 * Sets the types to the specified value.
	 * 
	 * @param types
	 *        the types to set
	 */
	@SuppressWarnings("unchecked")
	public void setTypes(Class<?>... types) {
		List<Class<? extends IJsonNode>> typeList = new ArrayList<Class<? extends IJsonNode>>();
		for (Class<?> type : types) {
			if (!IJsonNode.class.isAssignableFrom(type))
				throw new IllegalArgumentException();
			typeList.add((Class<? extends IJsonNode>) type);
		}
		setTypes(typeList);
	}

	/**
	 * Sets the types to the specified value.
	 * 
	 * @param types
	 *        the types to set
	 */
	public CsvFormat withTypes(Class<?>... types) {
		setTypes(types);
		return this;
	}

	/**
	 * Returns the types.
	 * 
	 * @return the types
	 */
	public List<Class<? extends IJsonNode>> getTypes() {
		return this.types;
	}

	/**
	 * Sets the numLineSamples to the specified value.
	 * 
	 * @param numLineSamples
	 *        the numLineSamples to set
	 */
	public void setNumLineSamples(final int numLineSamples) {
		if (numLineSamples <= 0)
			throw new IllegalArgumentException("numLineSamples must be positive");

		this.numLineSamples = numLineSamples;
	}

	/**
	 * Sets the quotation to the specified value.
	 * 
	 * @param quotation
	 *        the quotation to set
	 */
	@Property
	@Name(verb = "quote")
	public void setQuotation(final Boolean quotation) {
		this.quotation = quotation;
	}

	public CsvFormat withFieldDelimiter(final String fieldDelimiter) {
		this.setFieldDelimiter(fieldDelimiter);
		return this;
	}

	/**
	 * Sets the quotation to the specified value.
	 * 
	 * @param quotation
	 *        the quotation to set
	 */
	@Property
	@Name(verb = "header")
	public void setHeader(final Boolean header) {
		this.header = header;
	}

	public CsvFormat withHeader(final Boolean header) {
		setHeader(header);
		return this;
	}

	/**
	 * Sets the keyNames to the specified value.
	 * 
	 * @param keyNames
	 *        the keyNames to set
	 */
	public CsvFormat withKeyNames(final List<String> keyNames) {
		this.setKeyNames(keyNames);
		return this;
	}

	/**
	 * Sets the keyNames to the specified value.
	 * 
	 * @param keyNames
	 *        the keyNames to set
	 */
	public CsvFormat withKeyNames(final String... keyNames) {
		this.setKeyNames(keyNames);
		return this;
	}

	/**
	 * Sets the quotation to the specified value.
	 * 
	 * @param quotation
	 *        the quotation to set
	 */
	public CsvFormat withQuotation(final Boolean quotation) {
		this.setQuotation(quotation);
		return this;
	}

	@Override
	protected String[] getPreferredFilenameExtensions() {
		return new String[] { "csv", "tsv" };
	}

	private void configureDelimiter(final String outputPath) {
		if (this.fieldDelimiter == AUTO)
			try {
				this.fieldDelimiter = new URI(outputPath).getPath().endsWith(".tsv") ? '\t' : ',';
			} catch (final URISyntaxException e) {
				// cannot happen - path has been validated by operator
			}
	}

	static char inferFieldDelimiter(final Path path) {
		return path.getName().endsWith("tsv") ? '\t' : ',';
	}

	/**
	 */
	public static class CountingReader extends Reader {
		private long absolutePos = 0, limit = 0;

		private boolean reachedLimit = false, eos = false;

		private final ByteBuffer streamBuffer = ByteBuffer.allocate(100);

		private final CharBuffer charBuffer = CharBuffer.allocate(100);

		private final FSDataInputStream stream;

		private CharsetDecoder decoder;

		private final Charset cs;

		public CountingReader(final FSDataInputStream stream, final String charset, final long limit) {
			this.stream = stream;
			this.cs = Charset.forName(charset);
			this.decoder = this.cs.newDecoder();
			this.limit = limit;
			// mark as empty
			this.charBuffer.limit(0);
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.Reader#close()
		 */
		@Override
		public void close() throws IOException {
			this.stream.close();
		}

		public void liftLimit() {
			this.limit = Long.MAX_VALUE;
			this.reachedLimit = this.eos = false;
		}

		public boolean reachedLimit() {
			return this.reachedLimit;
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.Reader#read()
		 */
		@Override
		public int read() throws IOException {
			if (this.charBuffer.remaining() == 0) {
				if (!this.eos)
					this.fillCharBufferIfEmpty();
				if (this.eos)
					return -1;
			}
			return this.charBuffer.get();
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.Reader#read(char[], int, int)
		 */
		@Override
		public int read(final char[] cbuf, final int off, final int len) throws IOException {
			int toRead = len - off;
			while (toRead > 0) {
				this.fillCharBufferIfEmpty();
				final int currentReadCount = Math.min(toRead, this.charBuffer.length());
				this.charBuffer.get(cbuf, off, currentReadCount);
				toRead -= currentReadCount;
			}
			return len - toRead;
		}

		public void seek(final long absolutePos) throws IOException {
			this.absolutePos = absolutePos;
			this.stream.seek(absolutePos);
			// mark as empty
			this.charBuffer.limit(0);
			this.streamBuffer.clear();
			this.reachedLimit = this.eos = false;
			this.decoder = this.cs.newDecoder();
		}

		private void fillCharBufferIfEmpty() throws IOException {
			final int maxLen = this.streamBuffer.capacity();
			final int position = this.streamBuffer.position();
			final byte[] array = this.streamBuffer.array();
			final int read = this.stream.read(array, position,
				(int) Math.min(maxLen - position, this.limit - this.absolutePos));
			if (read <= 0) {
				this.reachedLimit = this.eos = true;
				return;
			}
			this.absolutePos += read;
			this.streamBuffer.position(0);
			this.streamBuffer.limit(position + read);
			this.reachedLimit = this.limit <= this.absolutePos;

			this.charBuffer.clear();
			final CoderResult coderResult = this.decoder.decode(this.streamBuffer, this.charBuffer, this.eos);
			this.charBuffer.flip();
			int undecodedBytes;
			if (coderResult == CoderResult.UNDERFLOW && !this.eos &&
				(undecodedBytes = this.streamBuffer.remaining()) > 0) {
				System.arraycopy(array, maxLen - undecodedBytes, array, 0, undecodedBytes);
				this.streamBuffer.limit(maxLen);
				this.streamBuffer.position(undecodedBytes);
				this.absolutePos -= undecodedBytes;
				// this.charBuffer.limit(this.charBuffer.limit() - 1);
			} else
				this.streamBuffer.clear();
		}
	}

	/**
	 * InputFormat that interpretes the input data as a csv representation.
	 */
	public static class CsvInputFormat extends SopremoFileInputFormat {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4999295498719746952L;

		private char fieldDelimiter;

		private Boolean quotation, header;

		private boolean usesQuotation;

		private List<String> keyNames = new ArrayList<String>();

		private List<Class<? extends IJsonNode>> types = new ArrayList<Class<? extends IJsonNode>>();

		private List<NodeCache> coercingCaches = new ArrayList<NodeCache>();

		private TextNode coercingRawNode;

		private int numLineSamples;

		private final Deque<State> state = new LinkedList<State>();

		private CountingReader reader;

		private final IObjectNode objectNode = new ObjectNode();

		private final StringBuilder builder = new StringBuilder();

		private char unicodeChar, unicodeCount;

		private long pos = 0;

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.api.io .FileInputFormat#close()
		 */
		@Override
		public void close() throws IOException {
			this.revertToPreviousState();
			this.reader.close();
			super.close();
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.io.SopremoFormat.SopremoFileInputFormat#nextValue()
		 */
		@Override
		public IJsonNode nextValue() throws IOException {
			int lastCharacter, fieldIndex = 0;
			boolean lastValue = false;
			this.objectNode.clear();
			do {
				lastCharacter = this.fillBuilderWithNextField();
				if (lastCharacter == -1 && !lastValue) {
					// ignore empty line
					if (this.builder.length() == 0 && fieldIndex == 0)
						break;

					lastValue = true;
					// read the remainder of the started line
					this.reader.liftLimit();
					lastCharacter = 0;
					continue;
				}
				this.addToObject(fieldIndex++, this.builder.toString());
				this.builder.setLength(0);
			} while (lastCharacter != '\n' && lastCharacter != -1);

			if (lastCharacter == -1 || lastValue)
				this.endReached();
			if (this.objectNode.size() == 0)
				return null;
			return this.objectNode;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.io.SopremoFormat.SopremoFileInputFormat#getAverageRecordBytes(java.util.ArrayList)
		 */
		@Override
		protected float getAverageRecordBytes(final FileSystem fs, final ArrayList<FileStatus> files,
				final long fileSize)
				throws IOException {
			// make the samples small for very small files
			int numSamples = Math.min(this.numLineSamples, (int) (fileSize / 1024));
			if (numSamples < 2)
				numSamples = 2;

			long offset = 0;
			long bytes = 0; // one byte for the line-break
			final long stepSize = fileSize / numSamples;

			int fileNum = 0;
			int samplesTaken = 0;

			// take the samples
			for (int sampleNum = 0; sampleNum < numSamples && fileNum < files.size(); sampleNum++) {
				FileStatus currentFile = files.get(fileNum);
				FSDataInputStream inStream = null;

				try {
					inStream = fs.open(currentFile.getPath());
					final LineReader lineReader = new LineReader(inStream, offset, currentFile.getLen() - offset, 1024);
					final byte[] line = lineReader.readLine();
					lineReader.close();

					if (line != null && line.length > 0) {
						samplesTaken++;
						bytes += line.length + 1; // one for the linebreak
					}
				} finally {
					// make a best effort to close
					if (inStream != null)
						try {
							inStream.close();
						} catch (final Throwable t) {
						}
				}

				offset += stepSize;

				// skip to the next file, if necessary
				while (fileNum < files.size() && offset >= (currentFile = files.get(fileNum)).getLen()) {
					offset -= currentFile.getLen();
					fileNum++;
				}
			}

			return bytes / (float) samplesTaken;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.io.SopremoFormat.SopremoFileInputFormat#configure(eu.stratosphere.configuration.
		 * Configuration)
		 */
		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			CollectionUtil.ensureSize(this.coercingCaches, this.types.size());
			boolean hasConversion = false;
			for (int index = 0; index < this.types.size(); index++)
				if (this.types.get(index) != TextNode.class) {
					this.coercingCaches.set(index, new NodeCache());
					hasConversion = true;
				}
			if(hasConversion)
				this.coercingRawNode = new TextNode();
		}

		@Override
		protected void open(final FSDataInputStream stream, final FileInputSplit split) throws IOException {
			this.setState(State.TOP_LEVEL);

			this.reader = new CountingReader(stream, this.getEncoding(), split.getStart() + split.getLength());
			this.usesQuotation = this.quotation == Boolean.TRUE;
			if (this.quotation == null) {
				// very simple heuristic
				for (int index = 0, ch; !this.usesQuotation && index < 1000 && (ch = this.reader.read()) != -1; index++)
					this.usesQuotation = ch == '"';

				this.reader.seek(this.splitStart);
			}

			if (this.keyNames.size() == 0) {
				if (split.getSplitNumber() > 0)
					this.reader.seek(0);
				this.pos = 0;
				this.keyNames = this.extractKeyNames();
			} else if (this.header == Boolean.TRUE && this.splitStart == 0)
				// skip header for first split, if we definitively know that there should be a header
				this.extractKeyNames();

			// skip to beginning of the first record
			if (this.splitStart > 0) {
				this.reader.seek(this.pos = this.splitStart - 1);
				if (this.usesQuotation) {
					// TODO: how to detect if where are inside a quotation?
					int ch;
					for (; (ch = this.reader.read()) != -1 && ch != '\n'; this.pos++)
						;
					if (ch == -1)
						this.endReached();
				} else {
					int ch;
					for (; (ch = this.reader.read()) != -1 && ch != '\n'; this.pos++)
						;
					if (ch == -1)
						this.endReached();
				}
			}
		}

		private void addToObject(final int fieldIndex, final String string) {
			if (fieldIndex < this.keyNames.size()) {
				Class<? extends IJsonNode> targetType = fieldIndex < this.types.size() ? this.types.get(fieldIndex) : TextNode.class;
				final IJsonNode node;
				if (targetType != TextNode.class) {
					this.coercingRawNode.setValue(string);
					node = TypeCoercer.INSTANCE.coerce(this.coercingRawNode, this.coercingCaches.get(fieldIndex), targetType);
				}
				else
					node = TextNode.valueOf(string);
				this.objectNode.put(this.keyNames.get(fieldIndex), node);
			}
		}

		/**
		 * Reads the key names from the first line of the first split.
		 */
		private List<String> extractKeyNames() throws IOException {
			final List<String> keyNames = new ArrayList<String>();
			int lastCharacter;
			do {
				lastCharacter = this.fillBuilderWithNextField();
				keyNames.add(this.builder.toString());
				this.builder.setLength(0);
			} while (lastCharacter != -1 && lastCharacter != '\n');

			// we were explicitly told that there is no header, so this first line is actual data
			// we can use the data to get the number of columns and generate the keynames
			if (this.header == Boolean.FALSE) {
				for (int index = 0; index < keyNames.size(); index++)
					keyNames.set(index, String.valueOf(index));
				// we need to seek back to fetch the first row in the usual way
				if (this.splitStart == 0)
					this.reader.seek(0);
			}
			return keyNames;
		}

		private int fillBuilderWithNextField() throws IOException {
			int character = 0;
			readLoop: for (; (character = this.reader.read()) != -1; this.pos++) {
				final char ch = (char) character;
				switch (this.getCurrentState()) {
				case TOP_LEVEL:
					if (ch == this.fieldDelimiter) {
						this.builder.toString();
						break readLoop;
					} else if (ch == '\n') {
						final int lastCharPos = this.builder.length() - 1;
						if (lastCharPos >= 0 && this.builder.charAt(lastCharPos) == '\r')
							this.builder.setLength(lastCharPos);
						break readLoop;
					} else if (this.usesQuotation && ch == '"')
						this.setState(State.QUOTED);
					else
						this.builder.append(ch);
					break;
				case ESCAPED:
					if (ch == 'u')
						this.setState(State.UNICODE);
					else {
						this.builder.append(ch);
						this.revertToPreviousState();
					}
					break;
				case QUOTED:
					switch (ch) {
					case '"':
						this.revertToPreviousState();
						break;
					case '\\':
						this.setState(State.ESCAPED);
						break;
					default:
						this.builder.append(ch);
					}
					break;
				case UNICODE:
					final int digit = Character.digit(ch, 16);
					if (digit != -1)
						this.unicodeChar = (char) (this.unicodeChar << 4 | digit);
					else
						throw new IOException("Cannot parse unicode character at position: " + this.pos +
							" split start: " + this.splitStart);
					if (++this.unicodeCount >= 4) {
						this.builder.append(this.unicodeChar);
						this.unicodeChar = 0;
						this.unicodeCount = 0;
						this.revertToPreviousState();
						this.revertToPreviousState();
					}
					break;
				}
			}
			return character;
		}

		/**
		 * @return
		 */
		private State getCurrentState() {
			return this.state.peek();
		}

		private State revertToPreviousState() {
			return this.state.pop();
		}

		/**
		 * @param escaped
		 */
		private void setState(final State newState) {
			this.state.push(newState);
		}

	}

	public static class CsvOutputFormat extends SopremoFileOutputFormat {
		/**
		 * 
		 */
		private static final long serialVersionUID = 385038405770899069L;

		private char fieldDelimiter;

		private List<String> keyNames;

		private Boolean quotation, header;

		private Writer writer;

		private transient IntList escapePositions = new IntArrayList();

		private boolean writeHeader;

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.api.io .FileOutputFormat#close()
		 */
		@Override
		public void close() throws IOException {
			this.writer.close();
			super.close();
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.io.SopremoFormat.SopremoFileOutputFormat#writeValue(eu.stratosphere.sopremo.type.
		 * IJsonNode)
		 */
		@Override
		public void writeValue(final IJsonNode value) throws IOException {
			if (this.writeHeader)
				this.writerHeader(value);
			if (this.keyNames.isEmpty())
				if (value instanceof IArrayNode<?>) {
					this.writeArray(value);
					return;
				}
				else if (!(value instanceof IObjectNode)) {
					this.write(value);
					this.writeLineTerminator();
					return;
				} else
					this.detectKeyNames(value);
			this.writeObject((IObjectNode) value);
		}

		private void writerHeader(IJsonNode value) throws IOException {
			this.writeHeader = false;
			TextNode field = new TextNode();
			if (this.keyNames.isEmpty()) {
				if (value instanceof IArrayNode<?>) {
					for (int index = 0; index < ((IArrayNode<?>) value).size(); index++) {
						field.clear();
						field.append(index);
						writeValue(field);
					}
					this.writeLineTerminator();
					return;
				}
				else if (!(value instanceof IObjectNode)) {
					field.append("Value");
					this.write(field);
					this.writeLineTerminator();
					return;
				} else
					this.detectKeyNames(value);
			}
			for (int index = 0; index < this.keyNames.size(); index++) {
				field.setValue(this.keyNames.get(index));
				writeValue(field);
			}
			this.writeLineTerminator();
		}

		protected void detectKeyNames(final IJsonNode value) {
			final List<Entry<String, IJsonNode>> values = Lists.newArrayList(((IObjectNode) value).iterator());
			this.keyNames = new ArrayList<String>(values.size());
			for (int index = 0; index < values.size(); index++)
				this.keyNames.add(values.get(index).getKey());
			if (this.keyNames.isEmpty())
				throw new IllegalStateException("Found empty object and cannot detect key names");
		}

		protected String escapeString(String string) {
			this.escapePositions.clear();
			for (int index = 0, count = string.length(); index < count; index++) {
				final char ch = string.charAt(index);
				if (ch == '\"' || ch == '\\')
					this.escapePositions.add(index);
			}

			if (this.escapePositions.size() > 0) {
				final char[] source = string.toCharArray();
				final char[] result = new char[string.length() + this.escapePositions.size()];

				int srcPos = 0;
				for (int index = 0, size = this.escapePositions.size(); index < size; index++) {
					final int endPos = this.escapePositions.getInt(index);
					final int length = endPos - srcPos;
					final int targetPos = srcPos + index;
					System.arraycopy(source, srcPos, result, targetPos, length);
					srcPos = endPos;
					result[targetPos + length] = '\\';
				}
				System.arraycopy(source, srcPos, result, srcPos + this.escapePositions.size(), source.length - srcPos);
				string = new String(result);
			}
			return string;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.io.SopremoFormat.SopremoFileOutputFormat#open(eu.stratosphere.core.fs.
		 * FSDataOutputStream, int)
		 */
		@Override
		protected void open(final FSDataOutputStream stream, final int taskNumber) throws IOException {
			this.writer = new BufferedWriter(new OutputStreamWriter(stream, this.getEncoding()));
			this.writeHeader = taskNumber == 0 && this.header == Boolean.TRUE;
		}

		/**
		 * @param object
		 */
		private void write(final IJsonNode node) throws IOException {
			final String string = node.toString();
			if (this.quotation != Boolean.FALSE) {
				this.writer.write('"');
				this.writer.write(this.escapeString(string));
				this.writer.write('"');
			} else
				this.writer.write(string);
		}

		private void writeArray(final IJsonNode value) throws IOException {
			final IArrayNode<?> array = (IArrayNode<?>) value;
			if (!array.isEmpty()) {
				this.write(array.get(0));
				for (int index = 1; index < array.size(); index++) {
					this.writeSeparator();
					this.write(array.get(index));
				}
			}
			this.writeLineTerminator();
		}

		/**
		 * 
		 */
		private void writeLineTerminator() throws IOException {
			this.writer.write('\n');
		}

		/**
		 * @param value
		 */
		private void writeObject(final IObjectNode value) throws IOException {
			this.write(value.get(this.keyNames.get(0)));
			for (int index = 1; index < this.keyNames.size(); index++) {
				this.writeSeparator();
				this.write(value.get(this.keyNames.get(index)));
			}
			this.writeLineTerminator();
		}

		/**
		 * 
		 */
		private void writeSeparator() throws IOException {
			this.writer.write(this.fieldDelimiter);
		}
	}

	private enum State {
		TOP_LEVEL, QUOTED, ESCAPED, UNICODE;
	}
}