package io.digdag.standards.operator.jdbc;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.Exchanger;

public class CSVWriter
    implements AutoCloseable
{
    private final Writer out;

    public CSVWriter(Writer out)
    {
        this.out = out;
    }

    public void addCsvHeader(List<String> columnNames)
            throws IOException
    {
        boolean first = true;
        for (String columnName : columnNames) {
            if (first) { first = false; }
            else { out.write(DELIMITER_CHAR); }
            addCsvText(columnName);
        }
        out.write("\r\n");
    }

    public void addCsvRow(List<TypeGroup> typeGroups, List<Object> row)
            throws IOException
    {
        for (int i = 0; i < typeGroups.size(); i++) {
            if (i > 0) {
                out.write(DELIMITER_CHAR);
            }
            Object v = row.get(i);
            if (v == null) {
                continue;
            }

            if (typeGroups.get(i) == TypeGroup.STRING) {
                addCsvText(v.toString());
            }
            else {
                addCsvText(v.toString());
            }
        }
        out.write("\r\n");
    }

    public void addCsvText(String value)
            throws IOException
    {
        out.write(escapeAndQuoteCsvValue(value));
    }

    private static final char DELIMITER_CHAR = ',';
    private static final char ESCAPE_CHAR = '"';
    private static final char QUOTE_CHAR = '"';

    public String escapeAndQuoteCsvValue(String v)
    {
        if (v.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(QUOTE_CHAR);
            sb.append(QUOTE_CHAR);
            return sb.toString();
        }

        StringBuilder escapedValue = new StringBuilder();
        char previousChar = ' ';

        boolean isRequireQuote = false;

        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);

            if (c == QUOTE_CHAR) {
                escapedValue.append(ESCAPE_CHAR);
                escapedValue.append(c);
                isRequireQuote = true;
            }
            else if (c == '\r') {
                escapedValue.append('\n');
                isRequireQuote = true;
            }
            else if (c == '\n') {
                if (previousChar != '\r') {
                    escapedValue.append('\n');
                    isRequireQuote = true;
                }
            }
            else if (c == DELIMITER_CHAR) {
                escapedValue.append(c);
                isRequireQuote = true;
            }
            else {
                escapedValue.append(c);
            }
            previousChar = c;
        }

        if (isRequireQuote) {
            StringBuilder sb = new StringBuilder();
            sb.append(QUOTE_CHAR);
            sb.append(escapedValue);
            sb.append(QUOTE_CHAR);
            return sb.toString();
        }
        else {
            return escapedValue.toString();
        }
    }

    @Override
    public void close()
            throws Exception
    {
        out.close();
    }

    @Override
    public String toString()
    {
        return "CSVWriter{" +
                "out=" + out +
                '}';
    }
}
