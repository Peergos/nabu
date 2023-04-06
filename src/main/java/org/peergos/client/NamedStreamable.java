package org.peergos.client;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface NamedStreamable
{
    InputStream getInputStream() throws IOException;

    Optional<String> getName();

    List<NamedStreamable> getChildren();

    boolean isDirectory();

    default byte[] getContents() throws IOException {
        InputStream in = getInputStream();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        byte[] tmp = new byte[4096];
        int r;
        while ((r=in.read(tmp))>= 0)
            bout.write(tmp, 0, r);
        return bout.toByteArray();
    }

    class InputStreamWrapper implements NamedStreamable {
        private final Optional<String> name;
        private final InputStream data;

        public InputStreamWrapper(InputStream data) {
            this(Optional.empty(), data);
        }

        public InputStreamWrapper(String name, InputStream data) {
            this(Optional.of(name), data);
        }

        public InputStreamWrapper(Optional<String> name, InputStream data) {
            this.name = name;
            this.data = data;
        }

        public boolean isDirectory() {
            return false;
        }

        public InputStream getInputStream() {
            return data;
        }

        @Override
        public List<NamedStreamable> getChildren() {
            return Collections.emptyList();
        }

        public Optional<String> getName() {
            return name;
        }
    }

    class ByteArrayWrapper implements NamedStreamable {
        private final Optional<String> name;
        private final byte[] data;

        public ByteArrayWrapper(byte[] data) {
            this(Optional.empty(), data);
        }

        public ByteArrayWrapper(String name, byte[] data) {
            this(Optional.of(name), data);
        }

        public ByteArrayWrapper(Optional<String> name, byte[] data) {
            this.name = name;
            this.data = data;
        }

        public boolean isDirectory() {
            return false;
        }

        public InputStream getInputStream() throws IOException {
            return new ByteArrayInputStream(data);
        }

        @Override
        public List<NamedStreamable> getChildren() {
            return Collections.emptyList();
        }

        public Optional<String> getName() {
            return name;
        }
    }
}
