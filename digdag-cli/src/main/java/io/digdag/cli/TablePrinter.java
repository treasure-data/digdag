package io.digdag.cli;

import com.google.common.collect.ImmutableList;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

class TablePrinter
{
    private final PrintStream out;
    private final List<List<String>> rows = new ArrayList<>();
    private final List<Integer> widths = new ArrayList<>();
    private final int margin;

    public TablePrinter(PrintStream out)
    {
        this.out = out;
        this.margin = 2;
    }

    void row(String... row)
    {
        row(asList(row));
    }

    void row(Collection<String> row)
    {
        List<String> r = ImmutableList.copyOf(row);
        grow(r);
        rows.add(r);
    }

    void print()
    {
        rows.forEach(this::printRow);
    }

    private void printRow(List<String> row)
    {

        for (int i = 0; i < widths.size(); i++) {
            int width = widths.get(i);
            String value = row.size() > i ? row.get(i) : "";
            int padding = width - value.length();
            out.print(value);
            for (int j = 0; j < padding; j++) {
                out.print(' ');
            }
            for (int j = 0; j < margin; j++) {
                out.print(' ');
            }
        }
        out.println();
    }

    private void grow(List<String> row)
    {
        for (int i = 0; i < row.size(); i++) {
            int length = row.get(i).length();
            if (widths.size() <= i) {
                widths.add(length);
            }
            else {
                int currentWidth = widths.get(i);
                int newWidth = Math.max(currentWidth, length);
                widths.set(i, newWidth);
            }
        }
    }
}
