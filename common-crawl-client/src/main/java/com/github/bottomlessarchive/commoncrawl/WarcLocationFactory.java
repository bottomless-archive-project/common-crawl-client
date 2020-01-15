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
import lombok.RequiredArgsConstructor;
import org.davidmoten.io.extras.IOUtil;

/**
 * This factory is responsible for creating the locations to the WARC files for a batch of Common Crawl corpus.
 *
 * @see <a href="http://commoncrawl.org/">Common Crawl</a>
 */
@RequiredArgsConstructor
public class WarcLocationFactory {

    private static final String AWS_S3_PREFIX = "https://commoncrawl.s3.amazonaws.com/";

    /**
     * Return the locations of the WARC files as {@link URL}s that belong to the provided Common Crawl crawl id.
     *
     * @param crawlId the id of the Common Crawl crawl to get the locations for
     * @return the locations of the WARC files
     */
    public List<URL> newUrls(final String crawlId) {
        return newLocations(crawlId).stream()
            .map(location -> {
                try {
                    return new URL(location);
                } catch (final MalformedURLException e) {
                    //Should never be thrown
                    throw new RuntimeException("Unable to convert WARC url: " + location + "!", e);
                }
            })
            .collect(Collectors.toList());
    }

    /**
     * Return the locations of the WARC files that belong to the provided Common Crawl crawl id.
     *
     * @param crawlId the id of the Common Crawl crawl to get the locations for
     * @return the locations of the WARC files
     */
    public List<String> newLocations(final String crawlId) {
        try (final BufferedReader downloadPathsReader = downloadLocations(crawlId)) {
            return downloadPathsReader.lines()
                .map(this::buildLocation)
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Unable to load WARC file paths.", e);
        }
    }

    private BufferedReader downloadLocations(final String pathsLocation) throws IOException {
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
