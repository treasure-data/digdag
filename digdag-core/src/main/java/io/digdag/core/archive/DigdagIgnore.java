package io.digdag.core.archive;

import org.eclipse.jgit.ignore.IgnoreNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Optional;

public class DigdagIgnore implements DirectoryStream.Filter<Path> {
    private Path projectPath;
    private IgnoreNode ignoreNode;

    private DigdagIgnore(){}

    public static Optional<DigdagIgnore> ofProject(Path projectPath) throws IOException {
        File digdagIgnoreFile = projectPath.resolve(".digdagignore").toFile();
        if (!digdagIgnoreFile.exists()) {
            return Optional.empty();
        }

        DigdagIgnore instance = new DigdagIgnore();
        instance.projectPath = projectPath;
        instance.ignoreNode = new IgnoreNode();
        try (FileInputStream s = new FileInputStream(digdagIgnoreFile)) {
            instance.ignoreNode.parse(s);
            return Optional.of(instance);
        }
    }

    @Override
    public boolean accept(Path target) {
        Boolean ignored = this.ignoreNode.checkIgnored(
                // checkIgnored assumes separator to be "/"
                this.projectPath.relativize(target).toString().replace(File.separatorChar, '/'),
                target.toFile().isDirectory()
        );
         return ignored == null || ignored == false;
    }
}
