use v6.d;

unit class App::MoarVM::HeapAnalyzer::Parser is export;

use App::MoarVM::HeapAnalyzer::LogTimelineSchema;
use Compress::Zstd;

class TocEntry {
    has Str $.kind;
    has Int $.position;
    has Int $.end;

    method new-from(blob8 $blob) {
        my $kind = no-nulls($blob.subbuf(0, 8).decode("utf8"));
        my $position = $blob.read-uint64(8);
        my $end = $blob.read-uint64(16);
        self.bless(:$kind, :$position, :$end);
    }

    method gist {
        " $.kind.fmt("%-9s") [$.position.fmt("%8x") - $.end.fmt("%8x") ($.length.fmt("%7x"))]"
    }
    method length( --> Int) {
        $.end - $.position
    }
}

has &.fh-factory;
has @!snapshot-tocs;

has @!stringheap;

method new($path) {
    self.bless(fh-factory =>
            -> $pos = 0 {
                my $fh = $path.IO.open(:r, :bin);
                $fh.seek($pos, SeekFromEnd) if $pos < 0;
                $fh.seek($pos, SeekFromBeginning) if $pos > 0;
                $fh
            });
}

sub no-nulls($str is copy) {
    if $str ~~ Blob {
        $str = $str.decode("utf8")
    }
    $str .= chop while $str.ends-with("\0");
    $str
}

method read-toc-contents(blob8 $toc) {
    do while $toc.elems > 8 {
        NEXT { $toc .= subbuf(24, *) }
        TocEntry.new-from($toc);
    }
}

method read-string-heap() {
    my \if = &.fh-factory.();

    for @!snapshot-tocs.map({ $_ with .first(*.kind eq "strings") }) {

        if.seek(.position);
        die "why are these not strings wtf" unless if.read(8).&no-nulls eq "strings";
        my $size = if.read(8).read-uint64(0);
        use Compress::Zstd;

        my Zstd::Decompressor $decomp .= new;
        my buf8 $result;

        while not $decomp.finished-a-frame {
            my $read = if.read($decomp.suggested-next-size);
            $result = $decomp.decompress($read);
        }

        my $leftover-length = (my $leftovers = $decomp.get-leftovers()).elems;
        #say "position after string heap was read: ", (if.tell - $leftover-length).fmt("%x"), " toc end was ", .end.fmt("%x");
        #my $extraread = $leftovers.subbuf(0, 32);
        #say "extra data after string heap was read: ", $extraread.decode("utf8-c8");

        my $strings-pushed = 0;
        while $result.elems > 0 {
            my $strlen = $result.read-uint32(0);
            my $string = $result.subbuf(4, $strlen).decode("utf8");
            @!stringheap.push: $string;
            $result.splice(0, 4 + $strlen);
            $strings-pushed++;
        }
    }
    @!stringheap
}

method read-staticframes() {
    my %interesting-kinds is Set = "sfname", "sfcuid", "sfline", "sffile";
    my %tocs-per-kind;
    for @!snapshot-tocs.map({ $^toc with $^toc.first({ .kind ~~ %interesting-kinds.keys.any }) }) {
        my @tocs = .grep({ .kind ~~ %interesting-kinds.keys.any });

        %tocs-per-kind{.kind}.push($_) for @tocs;
    }

    my %results = :sfname, :sfcuid, :sfline, :sffile;

    await do for %interesting-kinds.keys -> $kindname {
        .receive with $*TOKEN-POOL;

        start {
            my @values;
            if $kindname eq "sfline" { @values := my int32 @ }
            else { @values := my int @ }

            my \if = &.fh-factory.();
            my Zstd::InBuffer $input-buffer .= new;
            my Zstd::OutBuffer $output-buffer .= new;
            for %tocs-per-kind{$kindname}.pairs -> $p {
                self!read-attribute-stream($kindname, $p.value, if => if, :@values, :$input-buffer, :$output-buffer);
            }

            %results{$kindname} := @values;
            LEAVE .send(True) with $*TOKEN-POOL;
            CATCH { note "$kindname exception: $_" }
        }
    }

    %results;
}

method read-types() {
    my %interesting-kinds is Set = "reprname", "typename";
    my %tocs-per-kind;
    for @!snapshot-tocs.map({ $^toc with $^toc.first({ .kind ~~ %interesting-kinds.keys.any }) }) {
        my @tocs = .grep({ .kind ~~ %interesting-kinds.keys.any });

        %tocs-per-kind{.kind}.push($_) for @tocs;
    }

    my %results = :reprname, :typename;

    await do for %interesting-kinds.keys -> $kindname {
        .receive with $*TOKEN-POOL;
        start {
            my \if = &.fh-factory.();
            my int @values;
            my Zstd::InBuffer $input-buffer .= new;
            my Zstd::OutBuffer $output-buffer .= new;
            for %tocs-per-kind{$kindname}.pairs -> $p {
                self!read-attribute-stream($kindname, $p.value, if => if, :@values, :$input-buffer, :$output-buffer);
            }
            %results{$kindname} := @values;
            LEAVE .send(True) with $*TOKEN-POOL;
            CATCH { note "$kindname exception: $_" }
        }
    }

    %results;
}


method !read-attribute-stream($kindname, $toc, :$values is copy, :$if = &.fh-factory.(), :$input-buffer, :$output-buffer) {
    App::MoarVM::HeapAnalyzer::Log::ParseAttributeStream.log: kind => $kindname, position => $toc.position.fmt("%x"), {
        my $realstart = now;
        my \if := $if;
        if.seek($toc.position);

        die "that's not the kind i'm looking for?!" unless if.read(8).&no-nulls eq $kindname;

        my $entrysize = if.read(2).read-uint16(0);
        my $size = if.read(8).read-uint64(0);

        my Zstd::Decompressor $decomp .= new(
                |%(:$input-buffer with $input-buffer),
                |%(:$output-buffer with $output-buffer),
                );
        my buf8 $result;

        while not $decomp.finished-a-frame {
            my $read = if.read($decomp.suggested-next-size);
            $result = $decomp.decompress($read);
        }

        my $leftover-length = (my $leftovers = $decomp.get-leftovers()).elems;
        #say "position after $kindname was read: ", (if.tell - $leftover-length).fmt("%x"), " toc end was ", $toc.end.fmt("%x");
        #my $extraread = $leftovers.subbuf(0, 32);
        #say "extra data after $kindname was read: ", $extraread.decode("utf8-c8");

        without $values {
            if $entrysize == 2 {
                $values = my uint16 @;
            }
            elsif $entrysize == 4 {
                $values = my uint32 @;
            }
            elsif $entrysize == 8 {
                $values = my uint64 @;
            }
            else {
                note "what $entrysize $kindname";
            }
        }

        my $original-size = $values.elems;

        $values[$original-size + $result.elems div $entrysize] = 0;

        #note $result.elems div $entrysize, " entries for $kindname";

        #say $values.^name, " ", $kindname;

        use nqp;
        my $start = now;
        my int $pos = 0;
        my int $endpos = $result.elems div $entrysize + $pos;
        if $entrysize == 2 {
            repeat {
                nqp::bindpos_i(nqp::decont($values), $pos + $original-size, $result.read-uint16(nqp::mul_i($pos, 2)));
            } while ($pos++ < $endpos - 1);
        }
        elsif $entrysize == 4 {
            repeat {
                nqp::bindpos_i(nqp::decont($values), $pos + $original-size, $result.read-uint32(nqp::mul_i($pos, 4)));
            } while ($pos++ < $endpos - 1);
        }
        elsif $entrysize == 8 {
            repeat {
                nqp::bindpos_i(nqp::decont($values), $pos + $original-size, $result.read-uint64(nqp::mul_i($pos, 8)));
            } while ($pos++ < $endpos - 1);
        }
        else {
            note "what size is $entrysize wtf $kindname";
        }

        #note "splitting apart $kindname took $( my $split-time = now - $start )s; total work time $( my $all-time = now - $realstart ) ({ $split-time * 100 / $all-time }% splitting";

        $values<>;
    }
}

method fetch-collectable-data(
        :$toc, :$index,

        :@col-kinds!, :@col-desc-indexes!, :@col-size!, :@col-unmanaged-size!,
        :@col-refs-start!, :@col-num-refs!,

        :$num-objects! is rw,
        :$num-type-objects! is rw,
        :$num-stables! is rw,
        :$num-frames! is rw,
        :$total-size! is rw,

        :$progress
        ) {

    my %kinds-to-arrays = %(
            colkind => @col-kinds,
            colsize => @col-size,
            coltofi => @col-desc-indexes,
            colrfcnt => @col-num-refs,
            colrfstr => @col-refs-start,
            colusize => @col-unmanaged-size
            );

    my @interesting = $toc.grep(*.kind eq %kinds-to-arrays.keys.any);

    my Promise $kinds-promise .= new;
    my Promise $colsize-promise .= new;
    my Promise $colusize-promise .= new;

    my int64 $stat-total-size;
    my int64 $stat-total-usize;

    .increment-target with $progress;
    my $kind-stats-done = $kinds-promise.then({
        my $array = .result;
        my int $index = 0;
        my int $target = $array.elems;
        while $index < $target {
            my int $val = $array[$index++];
            if $val    == 1 { $num-objects++ }
            elsif $val == 2 { $num-type-objects++ }
            elsif $val == 3 { $num-stables++ }
            elsif $val == 4 { $num-frames++ }
        }
        .increment with $progress;
    });

    .add-target(2) with $progress;
    my $colsize-stats-done = $colsize-promise.then({
        my $array = .result;
        $stat-total-size = $array.sum;
        .increment with $progress;
    });


    my $colusize-stats-done = $colusize-promise.then({
        my $array = .result;
        $stat-total-usize = $array.sum;
        .increment with $progress;
    });

    await do for @interesting.list.sort(-*.length) {
        .increment-target with $progress;
        .receive with $*TOKEN-POOL;
        start {
            my $kindname = $_.kind;
            my $values := %kinds-to-arrays{.kind};
            self!read-attribute-stream(
                    .kind, $_, :$values
                    );
            if    .kind eq "colkind" { $kinds-promise.keep($values) }
            elsif .kind eq "colsize" { $colsize-promise.keep($values) }
            elsif .kind eq "colusize" { $colusize-promise.keep($values) }
            LEAVE { .send(True) with $*TOKEN-POOL; .increment with $progress }
            CATCH { note "$kindname exception: $_" }
        }
    }

    await $kind-stats-done, $colsize-stats-done, $colusize-stats-done;

    $total-size = $stat-total-size + $stat-total-usize;

    Nil
}

method fetch-references-data(
        :$toc, :$index,

        :@ref-kinds, :@ref-indexes, :@ref-tos,

        :$progress
        ) {

    my @interesting = $toc.grep(*.kind eq "refdescr" | "reftrget");

    await
        start {
            .increment-target with $progress;
            my $thetoc = @interesting.first(*.kind eq "refdescr");
            .receive with $*TOKEN-POOL;
            my $kindname = "refdescr";
            my $data = self!read-attribute-stream("refdescr", $thetoc);
            for $data.list -> uint64 $_ {
                @ref-kinds.push: $_ +& 0b11;
                @ref-indexes.push: $_ +> 2;
            }
            LEAVE { .send(True) with $*TOKEN-POOL; .increment with $progress }
            CATCH { note "$kindname exception: $_" }
        },
        start {
            note "increment target for reftrget";
            .increment-target with $progress;
            my $thetoc = @interesting.first(*.kind eq "reftrget");
            my $kindname = "reftrget";

            .receive with $*TOKEN-POOL;
            self!read-attribute-stream("reftrget", $thetoc, values => @ref-tos);
            LEAVE { .send(True) with $*TOKEN-POOL; .increment with $progress }
            CATCH { note "$kindname exception: $_" }
        };
}


method find-outer-toc {
    my \if = &.fh-factory.();

    # First, find the starting position of the outermost TOC.
    # Its position lives in the last 8 bytes of the file, hopefully.
    if.seek(-8, SeekFromEnd);
    if.seek(if.read(8).read-uint64(0), SeekFromBeginning);

    die "expected last 8 bytes of file to lead to a toc. alas..." unless no-nulls(if.read(8)) eq "toc";

    App::MoarVM::HeapAnalyzer::Log::ParseTOCs.log: {
        my $entries-to-read = if.read(8).read-uint64(0);
        my $toc = if.read($entries-to-read * 3 * 8);

        my @snapshot-tocs = self.read-toc-contents($toc);

        for @snapshot-tocs.head(*-1) {
            if.seek(.position);
            die "expected to find a toc here..." unless no-nulls(if.read(8)) eq "toc";
            App::MoarVM::HeapAnalyzer::Log::ParseTOCFound.log();
            my $size = if.read(8);
            my $innertoc = if.read(.end - .position - 16);
            my @inner-toc-entries = self.read-toc-contents($innertoc);

            @!snapshot-tocs.push(@inner-toc-entries);
        }
    }

    my $strings-promise = start App::MoarVM::HeapAnalyzer::Log::ParseStrings.log: {
        self.read-string-heap;
    }
    my $static-frames-promise = start App::MoarVM::HeapAnalyzer::Log::ParseStaticFrames.log: {
        self.read-staticframes;
    }
    my $types-promise = start App::MoarVM::HeapAnalyzer::Log::ParseTypes.log: {
        self.read-types;
    }

    return %(
            :$strings-promise,
            :$static-frames-promise,
            :$types-promise,
            snapshots => @!snapshot-tocs,
        );

    LEAVE { if.close }
}

