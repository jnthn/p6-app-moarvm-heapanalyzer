#`«
sub generate-snapshot-collection($seed = uint64.Range.pick) {
    srand($seed);
    my $snapshotcount = (^(^50).pick).pick + 1;

    my str @strings = flat("A".."Z", "a".."z", <\  - _>, "0".."9").roll((1..50).pick);

    my uint16 $sfcount = ^1024 .pick;

    my uint32 @staticframe-names;
    my uint32 @staticframe-cuids;
    my uint64 @staticframe-lines;
    my uint32 @staticframe-files;

    @staticframe-names.append((^@strings.elems).roll($sfcount));
    @staticframe-cuids.append((^@strings.elems).roll($sfcount));
    @staticframe-lines.append(uint64.pick.pick.pick.pick xx $sfcount);
    @staticframe-files.append((^@strings.elems).roll($sfcount));

    my $old-staticframe-count = 0;
    my $old-strings-count = 0;

    my @snapshots = do
        for $snapshotcount {
            %(
                seed => ((my $seed = uint64.Range.pick; srand($seed); $seed)),
                staticframe-names => @staticframe-names.skip($old-staticframe-count).Array,
                staticframe-cuids => @staticframe-cuids.skip($old-staticframe-count).Array,
                staticframe-lines => @staticframe-lines.skip($old-staticframe-count).Array,
                staticframe-files => @staticframe-files.skip($old-staticframe-count).Array,
                strings => @strings.skip($old-strings-count)
            );
            
        }

    @snapshots;
}
#`»

use Compress::Zlib;

my sub the-gzslurp($filename, :$bin) {
    my $proc = run("zcat", $filename, :out, |%(:$bin if $bin));
    LEAVE $proc.out.close;
    if $bin {
        $proc.out.slurp(:bin)
    }
    else {
        $proc.out.slurp()
    }
}

my $total-compressed-size;
my $total-uncompressed-size;

sub write-string-heap(\of, str @heap, @toc) {
    my $resbuf = buf8.new;
    for @heap {
        my $strbuf = buf8.new(0, 0, 0, 0, 0, 0, 0, 0);
        my $textbuf = .encode("utf8");
        $strbuf ~= $textbuf;
        $strbuf.write-uint64(0, $textbuf.elems);
        $resbuf.append($strbuf);
    }
    dd $resbuf;
    of.&write-gzipped-blob("strings", $resbuf, @toc);
}

sub transfer-string-heap(\of, @toc) {
    my @candidates = dir("/tmp", test => *.contains("heapdump_strings"));
    die "too many string heaps found in /tmp: @candidates.join(", ")" if @candidates > 1;
    my $data = the-gzslurp(@candidates.head, :bin);
    of.&write-gzipped-blob("strings", $data, @toc);
    CATCH { .say }
}

sub transfer-staticframes(\of, int $snap_idx, @toc) {
    die unless "/tmp/heapdump_names_$snap_idx.txt.gz".IO.e;
    of.&write-metadata-blob("sframes", blob8.new(0), @toc);
    for <names cuid line file>.hyper(:1batch).map({
        my $resultbuf = buf8.new;
        for the-gzslurp("/tmp/heapdump_$($_)_$snap_idx.txt.gz").lines {
            $resultbuf.write-uint64($++ * 8, $_.Int);
        }
        $_ => $resultbuf
    }) {
        of.&write-gzipped-blob(.key, .value, @toc);
    }
    CATCH { .say }
}

sub transfer-types(\of, int $snap_idx, @toc) {
    die unless "/tmp/heapdump_repr_$snap_idx.txt.gz".IO.e;
    of.&write-metadata-blob("types", blob8.new(0), @toc);
    hyper for <repr type>.hyper(:1batch).map({
        my $resultbuf = buf8.new;
        for the-gzslurp("/tmp/heapdump_$($_)_$snap_idx.txt.gz").lines {
            $resultbuf.write-uint64($++ * 8, $_.Int);
        }
        $_ => $resultbuf
    }) {
        of.&write-gzipped-blob(.key, .value, @toc);
    }
    CATCH { .say }
}

sub transfer-collectables(\of, int $snap_idx, @toc) {
    die unless "/tmp/heapdump_kind_$snap_idx.txt.gz".IO.e;
    of.&write-metadata-blob("clectbl", blob8.new(0), @toc);
    hyper for <kind tofi size unman refstart refcount>.hyper(:1batch).map({
        my $resultbuf = buf8.new;
        for the-gzslurp("/tmp/heapdump_$($_)_$snap_idx.txt.gz", :bin).decode("ascii").lines {
            $resultbuf.write-uint64($++ * 8, $_.Int);
        }
        $_ => $resultbuf
    }) {
        of.&write-gzipped-blob(.key, .value, @toc);
    }
    CATCH { .say }
}

sub transfer-refs(\of, int $snap_idx, @toc) {
    die unless "/tmp/heapdump_descrs_$snap_idx.txt.gz".IO.e;
    of.&write-metadata-blob("refs", blob8.new(0), @toc);
    hyper for <descrs kinds cindex>.hyper(:1batch).map({
        my $resultbuf = buf8.new;
        for the-gzslurp("/tmp/heapdump_$($_)_$snap_idx.txt.gz", :bin).decode("ascii").lines {
            $resultbuf.write-uint64($++ * 8, $_.Int);
        }
        $_ => $resultbuf;
    }) {
        of.&write-gzipped-blob(.key, .value, @toc);
    }
    CATCH { .say }
}

sub write-toc(\of, @innertoc, @toc?) {
    my $resultblob = buf8.new;
    my $tocstart = of.tell;

    for @innertoc -> ($type, $start, $end) {
        my $typebuf = buf8.new($type.encode("utf8").list);
        $typebuf[7] = 0;
        say $typebuf;
        $resultblob.append($typebuf);
        $resultblob.write-uint64($++ * 24 + 8, $start);
        $resultblob.write-uint64($++ * 24 + 16, $end);
    }
    $resultblob.write-uint64($resultblob.elems, $tocstart);

    of.&write-metadata-blob("toc", $resultblob, @toc);
}

sub write-text-blob(\of, Str $input, @toc) {
    my Buf $encoded = ("\n" ~ $input ~ "\n").encode("utf8") ~ buf8.new(0);
    my buf8 $result .= new;
    $result.write-uint32(0, $encoded.elems + 12);
    $result.append("plaintxt".encode("ascii"));
    $result.append($encoded);
    my $start = of.tell;
    of.write($result);
    my $end = of.tell;
    @toc.push(["plain", $start, $end]);
}

sub write-metadata-blob(\of, Str $type, blob8 $data, @toc?) {
    my $typebuf = buf8.new(0, 0, 0, 0) ~ $type.encode("utf8");
    die "type name must be at most 7 unicode bytes long" if $typebuf.elems > 12;
    $typebuf[4 + 7] = 0;
    $typebuf.write-uint32(0, $data.elems + 12);
    say "writing a metadata blob with $data.elems() elements\n";
    say $typebuf;
    say $data;
    my $start = of.tell();
    of.write($typebuf);
    of.write($data);
    my $end = of.tell();
    #$typebuf .= new;
    #$typebuf.write-uint64(0, $end);
    #of.write($typebuf);
    note "it lives at $start.fmt("%x")";
    @toc.push([$type, $start, $end]);
}

sub write-gzipped-blob(\of, Str $type, blob8 $data, @toc) {
    my $typebuf = buf8.new(0, 0, 0, 0) ~ $type.encode("utf8");
    die "type name must be at most 7 unicode bytes long" if $typebuf.elems > 12;
    $typebuf[4 + 7] = 0;
    my \zof = Compress::Zlib::Stream.new(:gzip);
    my $resultbuf = zof.deflate($data);
    $resultbuf.append(zof.finish);
    $typebuf.write-uint32(0, $resultbuf.elems + 12);
    my $start = of.tell();
    of.write($typebuf);
    of.write($resultbuf);
    my $end = of.tell();
    note "gzipped blob $type compressed to { $resultbuf.elems * 100 / $data.elems }%";
    note "it lives at $start.fmt("%x")";
    $total-compressed-size += $resultbuf.elems;
    $total-uncompressed-size += $data.elems;
    @toc.push([$type, $start, $end]);
}

sub no-nulls($str is copy) { $str .= chop while $str.ends-with("\0"); $str }

multi sub MAIN("read", $filename where .IO.f) {
    my \if = $filename.IO.open(:r, :bin);
    die "did not expect this to be the header ..." unless if.read("MoarHeapDumpv003".encode("latin1").elems) !eqv "MoarHeapDumpv003".encode("latin1");

    while if.read(4) -> $sizedata {
        my $blocksize = $sizedata.read-uint32(0);
        my $blocktype = no-nulls(if.read(8).decode("utf8"));
        my $at-start = if.tell;
        print "@", if.tell.fmt("%8x"), ": ";
        given $blocktype {
            when "toc" {
                say "toc: table of contents";
                my $toc = if.read($blocksize - 12);
                while $toc.elems > 8 {
                    my $name = no-nulls($toc.subbuf(0, 8).decode("utf8"));
                    my $from = $toc.read-uint64(8);
                    my $to = $toc.read-uint64(16);
                    $toc .= subbuf(24, *);
                    say "    ", [$name.fmt("%10s"), $from.fmt("%8x"), $to.fmt("%8x")];
                }
            }
            default {
                say "$blocktype.fmt("% 10s"): $blocksize.fmt("%8x")";
                if.read($blocksize - 12);
            }
        }
    }
}

multi sub MAIN("write", $filename) {
    my \of = $filename.IO.open(:w);

    of.print("MoarHeapDumpv003");

    my @toc;

    of.&write-text-blob(qq:to/COMMENT/, @toc);
        This file was created by MoarVM's Heap Snapshot Profiler.
        MoarVM is part of the Perl 6 project.
        This file is meant to be read by the moarvm heapanalyzer tool.
        It is made up of a series of "snapshots" that each contain a
        piece of String Heap, information about types, and static frames.
        Finally, the main part of each snapshot is collectables and
        references.  At the very end of the file, there is a TOC that helps
        skip to particular parts of the file.

        The TOC at the end of the file uses negative relative offsets so
        that starting from the end of the file, everything can be reached
        easily, and a heap snapshot file can be concatenated to the end of
        other file types.

        Every chunk of data starts with 32 bits of length field and seven
        utf8 bytes of text plus a null byte giving the kind of chunk it is.

        To conserve memory, the file is written with very little buffering.
        That's why some chunks of data will have a size of 0 bytes written
        in the file. In order to read past those pieces, use the TOC at the
        end of each snapshot or read bytes into a gzip decoder until the
        end of the stream is detected. This case can occur when the heap
        snapshot is written to a medium that doesn't support seeking, like
        a socket.
        COMMENT

    of.&write-text-blob("here comes collection number 1", @toc);
    of.&write-metadata-blob("colectn", "1: strings, staticframes, types, collectables, refs".encode("utf8"), @toc);
    of.&transfer-string-heap(@toc);

    for ^10 {
        my @innertoc;
        try of.&transfer-staticframes($_, @innertoc);
        try of.&transfer-types($_, @innertoc);
        try of.&transfer-collectables($_, @innertoc);
        try of.&transfer-refs($_, @innertoc);
        if @innertoc {
            of.&write-toc(@innertoc, @toc);
            say @innertoc;
        }
    }

    of.&write-text-blob(qq:to/COMMENT/, @toc);
        This file is a binary file created by MoarVM's heap snapshot profiler.

        It is not meant to be read by a human, but there are explanations of
        the file format in band with the rest of the data.

        Look for a program called App::MoarVM::HeapAnalyzer or moarperf.

        The next bit of the file is a table-of-contents for earlier parts of
        the file. It ends in a 64bit big endian unsigned integer that stores
        the current file pointer at the moment the integer itself was written
        out, and before that is one more such number storing where the file
        pointer was at the moment the TOC was written out.

        It points, among other things, at other TOCs that are formatted the
        same way.

        Most pieces in the file are gzipped with zlib.
        COMMENT

    of.&write-toc(@toc);

    of.close;

    .say for @toc;

    say "total   compressed size { $total-compressed-size / 1024 } KiB";
    say "total uncompressed size { $total-uncompressed-size / 1024 } KiB";

    say "";
    say "total ratio: { $total-compressed-size * 100 / $total-uncompressed-size }%";
}
