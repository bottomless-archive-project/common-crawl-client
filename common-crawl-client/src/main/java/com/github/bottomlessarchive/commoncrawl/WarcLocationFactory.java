package com.github.bottomlessarchive.commoncrawl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.davidmoten.io.extras.IOUtil;
import org.jetbrains.annotations.NotNull;

/**
 * This factory is responsible for creating the locations to the WARC files for a batch of Common Crawl corpus.
 *
 * @see <a href="http://commoncrawl.org/">Common Crawl</a>
 */
@RequiredArgsConstructor
public class WarcLocationFactory {

    private static final String AWS_S3_PREFIX = "https://commoncrawl.s3.amazonaws.com/";

    /**
     * Return the locations of the WARC files as a {@link List} of {@link URL}s that belong to the provided
     * Common Crawl crawl id.
     *
     * @param crawlId the id of the Common Crawl crawl to get the locations for
     * @return the locations of the WARC files
     */
    public List<URL> buildLocationUrlList(@NotNull @NonNull final String crawlId) {
        try (final Stream<URL> stream = buildLocationUrlStream(crawlId)) {
            return stream.collect(Collectors.toList());
        }
    }

    /**
     * Return the locations of the WARC files as a {@link Stream} of {@link URL}s that belong to the provided
     * Common Crawl crawl id.
     * <p>
     * The returned stream should by closed by the caller of this method.
     *
     * @param crawlId the id of the Common Crawl crawl to get the locations for
     * @return the locations of the WARC files
     */
    public Stream<URL> buildLocationUrlStream(@NotNull @NonNull final String crawlId) {
        return buildLocationStringStream(crawlId)
            .map(location -> {
                try {
                    return new URL(location);
                } catch (final MalformedURLException e) {
                    //Should never be thrown
                    throw new RuntimeException("Unable to convert WARC url: " + location + "!", e);
                }
            });
    }

    /**
     * Return the locations of the WARC files that belong to the provided Common Crawl crawl id as a {@link List}.
     *
     * @param crawlId the id of the Common Crawl crawl to get the locations for
     * @return the locations of the WARC files
     */
    public List<String> buildLocationStringList(@NotNull @NonNull final String crawlId) {
        try (final Stream<String> stream = buildLocationStringStream(crawlId)) {
            return stream.collect(Collectors.toList());
        }
    }

    /**
     * Return the locations of the WARC files that belong to the provided Common Crawl crawl id as a {@link Stream}.
     * <p>
     * The returned stream should by closed by the caller of this method.
     *
     * @param crawlId the id of the Common Crawl crawl to get the locations for
     * @return the locations of the WARC files
     */
    public Stream<String> buildLocationStringStream(@NotNull @NonNull final String crawlId) {
        try {
            final BufferedReader downloadPathsReader = buildWarcReader(crawlId);

            return downloadPathsReader.lines()
                .map(this::buildLocation)
                .onClose(() -> {
                    try {
                        downloadPathsReader.close();
                    } catch (final IOException e) {
                        throw new RuntimeException("Unable to load WARC file paths.", e);
                    }
                });
        } catch (final IOException e) {
            throw new RuntimeException("Unable to load WARC file paths.", e);
        }
    }

    private BufferedReader buildWarcReader(final String pathsLocation) throws IOException {
        final InputStream warcPathLocation = IOUtil.gunzip(new URL(AWS_S3_PREFIX + "crawl-data/"
            + pathsLocation + "/warc.paths.gz").openStream());

        return new BufferedReader(new InputStreamReader(warcPathLocation, StandardCharsets.UTF_8));
    }

    private String buildLocation(final String partialLocation) {
        return new StringBuilder()
            .append(AWS_S3_PREFIX)
            .append(partialLocation)
            .toString();
    }
}
