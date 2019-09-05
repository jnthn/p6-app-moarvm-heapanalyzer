use v6.d;

unit class App::MoarVM::HeapAnalyzer::Model;

use Concurrent::Progress;

use nqp;

# We resolve the top-level data structures asynchronously.
has $!strings-promise;
has $!types-promise;
has $!static-frames-promise;

# Raw, unparsed, snapshot data.
has @!unparsed-snapshots;

# Promises that resolve to parsed snapshots.
has @!snapshot-promises;

has int $!version;

class Snapshot { ... }

enum Unit is export <Count Bytes CollectableId TypeName FrameName ReprName>;

role Result is export is rw {
    has Snapshot $.snapshot;

    has Range $.estimated-more = 0..Inf;
    has &.fetch-more = -> { False };
    has int $.batch-starts-at = 0;
}

class ResultTable does Result is export is rw {
    has Str @.headers;
    has Unit @.units;
    has @.values;
}

class ResultCollectablesDetails is export is rw {
    has Str @.headers;
    has Unit @.units;
    has @.values;

    has @.out-refs;
    has @.out-targets;
}

class ResultCollectablesList does Result is export is rw {
    has Str @.headers;
    has Unit @.units;
    has @.values;
}

class ResultPath does Result is export is rw {
    has Hash @.collectables;
    has Hash @.references;
}

# Holds and provides access to the types data set.
my class Types {
    has int @!repr-name-indexes;
    has int @!type-name-indexes;
    has @!strings;

    submethod BUILD(:@repr-name-indexes, int :@type-name-indexes, :@strings) {
        @!repr-name-indexes := @repr-name-indexes;
        @!type-name-indexes := @type-name-indexes;
        @!strings := @strings;
    }

    method repr-name(int $idx) {
        @!strings[@!repr-name-indexes[$idx]]
    }

    method type-name(int $idx) {
        @!strings[@!type-name-indexes[$idx]]
    }

    method all-with-type($name) {
        my int @found;
        with @!strings.first($name, :k) -> int $goal {
            my int $num-types = @!type-name-indexes.elems;
            loop (my int $i = 0; $i < $num-types; $i++) {
                @found.push($i) if @!type-name-indexes[$i] == $goal;
            }
        }
        @found
    }

    method all-with-repr($name) {
        my int @found;
        with @!strings.first($name, :k) -> int $goal {
            my int $num-types = @!repr-name-indexes.elems;
            loop (my int $i = 0; $i < $num-types; $i++) {
                @found.push($i) if @!repr-name-indexes[$i] == $goal;
            }
        }
        @found
    }
}

# Holds and provides access to the static frames data set.
my class StaticFrames {
    has int @!name-indexes;
    has int @!cuid-indexes;
    has int32 @!lines;
    has int @!file-indexes;
    has @!strings;

    submethod BUILD(:@name-indexes, :@cuid-indexes, :@lines, :@file-indexes, :@strings) {
        @!name-indexes := @name-indexes;
        @!cuid-indexes := @cuid-indexes;
        @!lines := @lines;
        @!file-indexes := @file-indexes;
        @!strings := @strings;
    }

    method summary(int $index) {
        my $name = @!strings[@!name-indexes[$index]] || '<anon>';
        my $line = @!lines[$index];
        my $path = @!strings[@!file-indexes[$index]];
        my $file = $path.split(/<[\\/]>/).tail;
        "$name ($file:$line)"
    }

    method all-with-name($name) {
        my int @found;
        with @!strings.first($name, :k) -> int $goal {
            my int $num-sf = @!name-indexes.elems;
            loop (my int $i = 0; $i < $num-sf; $i++) {
                @found.push($i) if @!name-indexes[$i] == $goal;
            }
        }
        if $name eq "<anon>" {
            my @more = self.all-with-name("");
            @found.splice(+@found, 0, @more);
        }
        @found
    }
}

# The various kinds of collectable.
my enum CollectableKind is export <<
    :Object(1) TypeObject STable Frame PermRoots InstanceRoots
    CStackRoots ThreadRoots Root InterGenerationalRoots CallStackRoots
>>;

my enum RefKind is export << :Unknown(0) Index String >>;

# Holds data about a snapshot and provides various query operations on it.
my class Snapshot {
    has int8 @!col-kinds;
    has int32 @!col-desc-indexes;
    has int16 @!col-size;
    has int32 @!col-unmanaged-size;
    has int32 @!col-refs-start;
    has int32 @!col-num-refs;

    has int @!col-revrefs-start;
    has int @!col-num-revrefs;

    has @!strings;
    has $!types;
    has $!static-frames;

    has $.num-objects;
    has $.num-type-objects;
    has $.num-stables;
    has $.num-frames;
    has $.total-size;

    has int8 @!ref-kinds;
    has int32 @!ref-indexes;
    has int32 @!ref-tos;

    has int @!revrefs-tos;

    has @!bfs-distances;
    has @!bfs-preds;
    has @!bfs-pred-refs;

    submethod BUILD(
        :@col-kinds, :@col-desc-indexes, :@col-size, :@col-unmanaged-size,
        :@col-refs-start, :@col-num-refs, :@strings, :$!types, :$!static-frames,
        :$!num-objects, :$!num-type-objects, :$!num-stables, :$!num-frames,
        :$!total-size, :@ref-kinds, :@ref-indexes, :@ref-tos
    ) {
        @!col-kinds := @col-kinds;
        @!col-desc-indexes := @col-desc-indexes;
        @!col-size := @col-size;
        @!col-unmanaged-size := @col-unmanaged-size;
        @!col-refs-start := @col-refs-start;
        @!col-num-refs := @col-num-refs;
        @!strings := @strings;
        @!ref-kinds := @ref-kinds;
        @!ref-indexes := @ref-indexes;
        @!ref-tos := @ref-tos;

        my $size = 0;
        for @!col-kinds, @!col-desc-indexes, @!col-size,
            @!col-unmanaged-size, @!col-refs-start, @!col-num-refs,
            @!strings, @!ref-kinds, @!ref-indexes, @!ref-tos {
            try $size += (($_.of.^nativesize // 64) div 8) * $_.elems()
        }
    }

    method forget() {
        @!col-kinds := my int8 @;
        @!col-desc-indexes = my int32 @;
        @!col-size = my int16 @;
        @!col-unmanaged-size = my int32 @;
        @!col-refs-start = my int32 @;
        @!col-num-refs = my int16 @;
        @!ref-kinds = my int8 @;
        @!ref-indexes = my int32 @;
        @!ref-tos = my int32 @;

        @!bfs-distances = my int @;
        @!bfs-preds = my int @;
        @!bfs-pred-refs = my int @;
    }

    method num-references() {
        @!ref-kinds.elems
    }

    method top-by-count(int $n, int $kind) {
        my %top;
        my int $num-cols = @!col-kinds.elems;
        loop (my int $i = 0; $i < $num-cols; $i++) {
            if @!col-kinds[$i] == $kind {
                %top{@!col-desc-indexes[$i]}++;
            }
        }
        self!munge-top-results(%top, $n, $kind)
    }

    method top-by-size(int $n, int $kind) {
        my %top;
        my int $num-cols = @!col-kinds.elems;
        loop (my int $i = 0; $i < $num-cols; $i++) {
            if @!col-kinds[$i] == $kind {
                %top{@!col-desc-indexes[$i]} += @!col-size[$i] + @!col-unmanaged-size[$i];
            }
        }
        self!munge-top-results(%top, $n, $kind)
    }
    
    method !munge-top-results(%top, int $n, int $kind) {
        my @raw-results = %top.sort(-*.value).head($n);
        if $kind == CollectableKind::Frame {
            @raw-results.map({
                [$!static-frames.summary(.key.Int), .value]
            })
        }
        else {
            @raw-results.map({
                [$!types.type-name(.key.Int), .value]
            })
        }
    }

    method find(int $n, int $kind, $cond, $value) {
        my int8 @matching;
        given $cond {
            when 'type' {
                @matching[$_] = 1 for $!types.all-with-type($value);
            }
            when 'repr' {
                @matching[$_] = 1 for $!types.all-with-repr($value);
            }
            when 'name' {
                @matching[$_] = 1 for $!static-frames.all-with-name($value);
            }
            default {
                die "Sorry, don't understand search condition $cond";
            }
        }

        my $result-obj =
                ResultCollectablesList.new(
                    :snapshot(self),
                    :headers("Object Id", "Description", "Unmanaged Size"),
                    :units(CollectableId,
                        $kind == CollectableKind::Frame
                            ?? FrameName
                            !! TypeName,
                        Bytes));

        my int $last-fetched = 0;
        my int $num-cols = @!col-kinds.elems;

        my &fetch-more = -> $count {
            my @results := $result-obj.values;
            $result-obj.batch-starts-at = +@results + 1;
            my int $targetsize = $count + $result-obj.batch-starts-at;
            loop (my int $i = $last-fetched;
                    $i < $num-cols && @results < $targetsize;
                    $i++) {
                if @!col-kinds[$i] == $kind && @matching[@!col-desc-indexes[$i]] {
                    @results.push: [
                        $i,
                        $kind == CollectableKind::Frame
                            ?? $!static-frames.summary(@!col-desc-indexes[$i])
                            !! $!types.type-name(@!col-desc-indexes[$i]),
                        @!col-size[$i] + @!col-unmanaged-size[$i]
                    ];
                }
            }
            $result-obj.estimated-more = 0 .. ($num-cols - $i);
            $last-fetched = $i;
        }
        fetch-more($n);
        $result-obj.fetch-more = &fetch-more;
        $result-obj;
    }

    method describe-col($cur-col) {
        unless $cur-col ~~ ^@!col-kinds.elems {
            die "No such collectable index $cur-col";
        }
        given @!col-kinds[$cur-col] {
            when Object {
                $!types.type-name(@!col-desc-indexes[$cur-col]) ~ ' (Object)'
            }
            when TypeObject {
                $!types.type-name(@!col-desc-indexes[$cur-col]) ~ ' (Type Object)'
            }
            when STable {
                $!types.type-name(@!col-desc-indexes[$cur-col]) ~ ' (STable)'
            }
            when Frame {
                $!static-frames.summary(@!col-desc-indexes[$cur-col]) ~ ' (Frame)'
            }
            when PermRoots { 'Permanent roots' }
            when InstanceRoots { 'VM Instance Roots' }
            when CStackRoots { 'C Stack Roots' }
            when ThreadRoots { 'Thread Roots' }
            when Root { 'Root' }
            when InterGenerationalRoots { 'Inter-generational Roots' }
            when CallStackRoots { 'Call Stack Roots' }
            default { '???' }
        }
    }

    method path($idx) {
        unless $idx ~~ ^@!col-kinds.elems {
            die "No such collectable index $idx";
        }
        self!ensure-bfs();

        my @path;
        my int $cur-col = $idx;
        until $cur-col == -1 {
            @path.unshift: self.describe-col($cur-col) ~ " ($cur-col)";

            my int $pred-ref = @!bfs-pred-refs[$cur-col];
            if $pred-ref >= 0 {
                @path.unshift: do given @!ref-kinds[$pred-ref] {
                    when String {
                        @!strings[@!ref-indexes[$pred-ref]]
                    }
                    when Index {
                        "Index @!ref-indexes[$pred-ref]"
                    }
                    default { 'Unknown' }
                }
            }

            $cur-col = @!bfs-preds[$cur-col];
        }

        @path
    }

    method details($idx) {
        unless $idx ~~ ^@!col-kinds.elems {
            die "No such collectable index $idx";
        }
        my @parts;

        @parts.push: self.describe-col($idx);

        my int $num-refs = @!col-num-refs[$idx];
        my int $refs-start = @!col-refs-start[$idx];
        loop (my int $i = 0; $i < $num-refs; $i++) {
            my int $ref-idx = $refs-start + $i;
            my int $to = @!ref-tos[$ref-idx];

            @parts.push: do given @!ref-kinds[$ref-idx] {
                when String {
                    @!strings[@!ref-indexes[$ref-idx]]
                }
                when Index {
                    "Index @!ref-indexes[$ref-idx]"
                }
                default { 'Unknown' }
            }
            @parts.push: self.describe-col($to) ~ " ($to)";
        }
        @parts;
    }

    method reverse-refs($idx) {
        self!ensure-incidents();

        say (0..^@!col-num-revrefs[$idx]) + @!col-revrefs-start[$idx];

        do for (0..^@!col-num-revrefs[$idx]) + @!col-revrefs-start[$idx] -> $r {
            my int $source = @!revrefs-tos[$r];
            self.describe-col($source) ~ " ($source)";
        }
    }

    method !ensure-bfs() {
        return if @!bfs-distances;

        my int32 @distances;
        my int @pred;
        my int @pred-ref;
        my int8 @color; # 0 = white, 1 = grey, 2 = black

        my int @delayed-string-refs;

        my Str @strings-to-slow-down = (
            "Inter-generational Roots",
            "Strings heap entry",
            "Boxed integer cache entry",
        );

        for @strings-to-slow-down {
            with @!strings.first($_, :k) {
                say @!strings[$_];
                @delayed-string-refs.push($_);
            }
        }

        #say @delayed-string-refs;

        @color[0] = 1;
        @distances[0] = 0;
        @pred[0] = -1;
        @pred-ref[0] = -1;

        my int @queue;
        my int @delayed-refs-queue;
        @queue.push(0);
        repeat {
            while @queue {
                my $cur-col = @queue.shift;
                my $num-refs = @!col-num-refs[$cur-col];
                my $refs-start = @!col-refs-start[$cur-col];
                my $refs-end = $refs-start + $num-refs;
                loop (my int $i = $refs-start; $i < $refs-end; $i++) {
                    my $ref-idx = $i;
                    my $to = @!ref-tos[$ref-idx];
                    my $ref-index = @!ref-indexes[$ref-idx];
                    if @color[$to] == 0 {
                        if @!ref-kinds[$ref-idx] == 2 && (
                                   $ref-index == @delayed-string-refs[0]
                                || $ref-index == @delayed-string-refs[1]
                                || $ref-index == @delayed-string-refs[2]
                            ) {
                            @delayed-refs-queue.push($cur-col);
                            @delayed-refs-queue.push($ref-idx);
                            @delayed-refs-queue.push($to);
                        }
                        else {
                            @color[$to] = 1;
                            @distances[$to] = @distances[$cur-col] + 1;
                            @pred[$to] = $cur-col;
                            @pred-ref[$to] = $ref-idx;
                            @queue.push($to);
                        }
                    }
                }
                @color[$cur-col] = 2;
            }
            if @delayed-refs-queue {
                repeat {
                    my  $cur-col = @delayed-refs-queue.shift;
                    my  $ref-idx = @delayed-refs-queue.shift;
                    my  $to =      @delayed-refs-queue.shift;

                    if @color[$to] == 0 {
                        @color[$to] = 1;
                        @distances[$to] = @distances[$cur-col] + 1;
                        @pred[$to] = $cur-col;
                        @pred-ref[$to] = $ref-idx;
                        @queue.push($to);
                    }
                } until @queue || !@delayed-refs-queue;
            }
        } until !@delayed-refs-queue && !@queue;

        @!bfs-distances := @distances;
        @!bfs-preds := @pred;
        @!bfs-pred-refs := @pred-ref;
    }

    method !ensure-incidents() {
        return if @!revrefs-tos;
        
        my num $start = now.Num;

        my int $num-coll = +@!col-kinds;
        my int $num-refs = +@!ref-tos;
        note "got $num-coll collectables to go through";

        my int @incoming-count;

        @incoming-count[$num-coll - 1] = 0;

        note "going through cols once { now - $start }";
        loop (my int $r = 0; $r < $num-refs; $r++) {
            @incoming-count[@!ref-tos[$r]]++;
        }

        my int $prefixsum;

        note "going through cols twice { now - $start }";
        loop (my int $c = 0; $c < $num-coll; $c++) {
            my int $count = @incoming-count[$c];
            @!col-revrefs-start[$c] = $prefixsum;
            @!col-num-revrefs[$c] = $count;
            $prefixsum += $count;
        }

        my int64 @cursors;
        @cursors[$num-coll - 1] = 0;

        note "going through cols three times { now - $start }";
        loop ($c = 0; $c < $num-coll; $c++) {
            my int $start = @!col-refs-start[$c];
            my int $last-ref = $start + @!col-num-refs[$c];
            loop (my int $r = $start; $r < $last-ref; $r++) {
                my int $to = nqp::atpos_i(@!ref-tos, $r);
                my int $targetpos = nqp::atpos_i(@!col-revrefs-start, $to) + nqp::atpos_i(@cursors, $to);
                @cursors.AT-POS($to)++;
                nqp::bindpos_i(@!revrefs-tos, $targetpos, $c);
            }
        }
        
        note "done { now - $start }";
    }
}

my class MyLittleBuffer {
    has $!buffer = Buf.new();
    has $.fh;

    method gimme(int $size) {
        if nqp::elems(nqp::decont($!buffer)) > $size {
            $!buffer;
        } else {
            $!buffer.splice(nqp::elems(nqp::decont($!buffer)), 0, $!fh.read(4096));
            $!buffer;
        }
    }

    method seek(|c) {
        $!fh.seek(|c);
        $!buffer = Buf.new();
    }

    method exactly(int $size) {
        self.gimme($size);
        Buf.new($!buffer.splice(0, $size));
    }
    method tell {
        $!fh.tell - nqp::elems(nqp::decont($!buffer))
    }
    method close() {
        $.fh.close;
        $!buffer = Buf.new;
    }
}

my int8 @empty-buf;
sub readSizedInt64(@buf) {
    #my $bytesize = 8;
    #my @buf := $fh.gimme(8);
    #die "expected $bytesize bytes, but got { @buf.elems() }" unless @buf.elems >= $bytesize;

    #my int64 $result = @buf.read-int64(0);
    #my int64 $result = nqp::readint(@buf,0,
          #BEGIN nqp::bitor_i(nqp::const::BINARY_SIZE_64_BIT,NativeEndian));


    my int64 $result =
            nqp::add_i nqp::shift_i(@buf),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf),  8),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 16),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 24),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 32),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 40),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 48),
                       nqp::bitshiftl_i(nqp::shift_i(@buf), 56);
    #@buf.splice(0, 8);
    #nqp::splice(@buf, @empty-buf, 0, 8);
    $result;
}
sub readSizedInt32(@buf) {
    #my $bytesize = 4;
    #my @buf := $fh.gimme(4);
    #die "expected $bytesize bytes, but got { @buf.elems() }" unless @buf.elems >= $bytesize;

    #my int64 $result = nqp::readint(@buf,0,
            #BEGIN nqp::bitor_i(nqp::const::BINARY_SIZE_32_BIT,NativeEndian));

    my int64 $result =
            nqp::add_i nqp::shift_i(@buf),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf),  8),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 16),
                       nqp::bitshiftl_i(nqp::shift_i(@buf), 24);

    #nqp::splice(@buf, @empty-buf, 0, 4);
    $result;

}
sub readSizedInt16(@buf) {
    #my $bytesize = 2;
    #my @buf := $fh.gimme(2);
    #die "expected $bytesize bytes, but got { @buf.elems() }" unless @buf.elems >= $bytesize;

    my int64 $result =
            nqp::add_i(nqp::shift_i(@buf),
                       nqp::bitshiftl_i(nqp::shift_i(@buf), 8));

    #my int64 $result = nqp::readint(@buf,0,
            #BEGIN nqp::bitor_i(nqp::const::BINARY_SIZE_16_BIT,NativeEndian));
    #nqp::splice(@buf, @empty-buf, 0, 2);
    $result;

}

submethod BUILD(IO::Path :$file = die "Must construct model with a file") {
    # Pull data from the file.
    my %top-level;
    my @snapshots;
    my $cur-snapshot-hash;

    $!version = 1;

    try {
        my $fh = $file.open(:r, :enc<latin1>);
        given $fh.readchars(16) {
            when "MoarHeapDumpv002" {
                $!version = 2;
            }
            when "MoarHeapDumpv003" {
                $!version = 3;
            }
        }
        LEAVE $fh.close;
        CATCH { .say }
    }

    if $!version == 1 {
        for $file.lines.kv -> $lineno, $_ {
            # Empty or comment
            when /^ \s* ['#' .*]? $/ {
                next;
            }

            # Data item
            when /^ (\w+) ':' \s*/ {
                my $key = ~$0;
                my $value = .substr($/.chars);
                with $cur-snapshot-hash {
                    .{$key} = $value;
                }
                else {
                    %top-level{$key} = $value;
                }
            }

            # Snapshot heading
            when /^ snapshot \s+ \d+ \s* $/ {
                push @snapshots, $cur-snapshot-hash := {};
            }

            # Confused
            default {
                die "Confused by heap snapshot line {$lineno + 1}";
            }
        }

        # Sanity check.
        sub want-key(%hash, $key, $where = "in the snapshot file header") {
            unless %hash{$key}:exists {
                die "Seems there's a missing $key entry $where"
            }
        }
        want-key(%top-level, 'strings');
        want-key(%top-level, 'types');
        want-key(%top-level, 'static_frames');
        for @snapshots.kv -> $idx, %snapshot {
            want-key(%snapshot, 'collectables', "in snapshot $idx");
            want-key(%snapshot, 'references', "in snapshot $idx");
        }

        # Set off background parsing of the headers, and stash unparsed snapshots.
        $!strings-promise = start from-json(%top-level<strings>).list;
        $!types-promise = start self!parse-types(%top-level<types>);
        $!static-frames-promise = start self!parse-static-frames(%top-level<static_frames>);
        @!unparsed-snapshots = @snapshots;
    }
    elsif $!version == 2 {
        my $fh = MyLittleBuffer.new(fh => $file.open(:r, :bin));
        constant index-entries = 4;
        constant per-snapshot-entries = 4;
        $fh.seek(-8 * index-entries, SeekFromEnd);
        my @sizes = readSizedInt64($fh.gimme(8)) xx index-entries;
        my ($stringheap_size, $types_size, $staticframe_size, $snapshot_entry_count) = @sizes;
        @sizes.pop; # remove the number of snapshot entries

        sub fh-at($pos) {
            my $fh = MyLittleBuffer.new(fh => $file.open(:r, :bin, :buffer(4096)));
            $fh.seek($pos, SeekFromBeginning);
            $fh
        }

        $fh.seek(-8 * index-entries - (8 * per-snapshot-entries) * $snapshot_entry_count, SeekFromEnd);
        my $snapshot-position = 16;
        @!unparsed-snapshots = do for ^$snapshot_entry_count -> $index {
            my @buf := $fh.gimme(per-snapshot-entries * 8);
            my @sizes = readSizedInt64(@buf), readSizedInt64(@buf), readSizedInt64(@buf), readSizedInt64(@buf);
            my $collpos = $snapshot-position;
            my $refspos = $collpos + @sizes[0];
            my $halfrefpos = $refspos + @sizes[2];
            my $incrementalpos = $refspos + @sizes[1];
            $snapshot-position += @sizes[0] + @sizes[1] + @sizes[3];
            {:$collpos, :$halfrefpos, :$refspos, :$incrementalpos, :$file, :$index};
        }

        my @positions = [\+] $snapshot-position, $stringheap_size, $types_size, $staticframe_size;
        my @fds = @positions.map(&fh-at);
        my ($stringheap_fd, $types_fd, $staticframe_fd, $snapshots_fd) = @fds;

        $!strings-promise       = start self!parse-strings-ver2($stringheap_fd);
        $!types-promise         = start self!parse-types-ver2($types_fd);
        $!static-frames-promise = start self!parse-static-frames-ver2($staticframe_fd);
    }
    elsif $!version == 3 {
        use App::MoarVM::HeapAnalyzer::Parser;

        my App::MoarVM::HeapAnalyzer::Parser $parser .= new($file);

        my %results := $parser.find-outer-toc;

        $!strings-promise = %results<strings-promise>;
        $!static-frames-promise = %results<static-frames-promise>.then({ given .result {
            StaticFrames.new(name-indexes => .<sfname>,
                    cuid-indexes => .<sfcuid>,
                    lines => .<sfline>,
                    file-indexes => .<sffile>,
                    strings => await $!strings-promise);
        }});
        $!types-promise = %results<types-promise>.then({ given .result {
            Types.new(repr-name-indexes => .<reprname>,
                      type-name-indexes => .<typename>,
                      strings => await $!strings-promise)
        }});
        @!unparsed-snapshots = do for %results<snapshots>.list.pairs {
            #say "unparsed snapshot: $_.key(): $_.value.perl()";
            %(:$parser, toc => .value, index => .key)
        }
    }
}

sub expect-header($fh, $name, $text = $name.substr(0, 4)) {
    my $result = $fh.exactly($text.chars).decode("latin1");
    die "expected the $name header at 0x{ ($fh.tell - $text.chars).base(16) }, but got $result.perl() instead." unless $result eq $text;
}

method !parse-strings-ver2($fh) {
    expect-header($fh, "strings", "strs");
    my $stringcount = readSizedInt64($fh.gimme(8));
    LEAVE { $fh.close }
    do for ^$stringcount {
        my $length = readSizedInt64($fh.gimme(8));
        if !$length { say "string index $_ is an empty string" }
        $length ?? $fh.exactly($length).decode("utf8")
                !! ""
    }
}
method !parse-types-ver2($fh) {
    expect-header($fh, "types");
    my ($typecount, $size-per-type) = readSizedInt64($fh.gimme(8)) xx 2;
    my int @repr-name-indexes;
    my int @type-name-indexes;
    for ^$typecount {
        my @buf := $fh.gimme(16);
        my int64 $repr-name-index = readSizedInt64(@buf);
        say "type index $_ has an empty repr name" if $repr-name-index == 6;
        my int64 $type-name-index = readSizedInt64(@buf);
        say "type index $_ has an empty type name (repr index $repr-name-index)" if $type-name-index == 6;
        @repr-name-indexes.push($repr-name-index);
        @type-name-indexes.push($type-name-index);
    }
    $fh.close;
    Types.new(:@repr-name-indexes, :@type-name-indexes, strings => await $!strings-promise);
}
method !parse-static-frames-ver2($fh) {
    expect-header($fh, "frames");
    my ($staticframecount, $size-per-frame) = readSizedInt64($fh.gimme(4)) xx 2;
    my int @name-indexes;
    my int @cuid-indexes;
    my int32 @lines;
    my int @file-indexes;
    for ^$staticframecount {
        my @buf := $fh.gimme(24);
        @name-indexes.push(readSizedInt64(@buf));
        @cuid-indexes.push(readSizedInt64(@buf));
        @lines       .push(readSizedInt64(@buf));
        @file-indexes.push(readSizedInt64(@buf));
    }
    $fh.close;
    StaticFrames.new(
        :@name-indexes, :@cuid-indexes, :@lines, :@file-indexes,
        strings => await $!strings-promise
    )
}


method !parse-types($types-str) {
    my int @repr-name-indexes;
    my int @type-name-indexes;
    for $types-str.split(';') {
        my @pieces := .split(',').List;
        @repr-name-indexes.push(@pieces[0].Int);
        @type-name-indexes.push(@pieces[1].Int);
    }
    Types.new(
        :@repr-name-indexes, :@type-name-indexes,
        strings => await $!strings-promise
    )
}

method !parse-static-frames($sf-str) {
    my int @name-indexes;
    my int @cuid-indexes;
    my int32 @lines;
    my int @file-indexes;
    for $sf-str.split(';') {
        my @pieces := .split(',').List;
        @name-indexes.push(@pieces[0].Int);
        @cuid-indexes.push(@pieces[1].Int);
        @lines.push(@pieces[2].Int);
        @file-indexes.push(@pieces[3].Int);
    }
    StaticFrames.new(
        :@name-indexes, :@cuid-indexes, :@lines, :@file-indexes,
        strings => await $!strings-promise
    )
}

method num-snapshots() {
    @!unparsed-snapshots.elems
}

enum SnapshotStatus is export <Preparing Ready Unprepared>;

method prepare-snapshot($index, :$updates) {
    with @!snapshot-promises[$index] -> $prom {
        if $updates {
            note "prepare-snapshot called with updates, but promise already exists";
            $updates.done();
        }
        given $prom.status {
            when Kept { Ready }
            when Broken { await $prom }
            default { Preparing }
        }
    }
    else {
        with @!unparsed-snapshots[$index] {
            note "---- ---- ----";
            note "prepare-snapshot called with ...updates? { so $updates }";
            note $_.perl;
            note "---- ---- ----";
            @!snapshot-promises[$index] = start self!parse-snapshot($_, :$updates);
            if $updates {
                $updates.emit({ index => $index, is-done => False }) if $updates;
            }
            Preparing
        }
        else {
            note "error: no such snapshot: $index";
            die "No such snapshot $index"
        }
    }
}

method snapshot-state($index) {
    with @!snapshot-promises[$index] -> $prom {
        given $prom.status {
            when Kept { Ready }
            when Broken { await $prom }
            default { Preparing }
        }
    }
    else {
        Unprepared
    }
}

method promise-snapshot($index, :$updates) {
    # XXX index checks
    die "no snapshot with index $index exists" unless @!unparsed-snapshots[$index]:exists;

    @!snapshot-promises[$index] //= start self!parse-snapshot(
        @!unparsed-snapshots[$index], :$updates
    )
}

method get-snapshot($index, :$updates) {
    # XXX index checks
    await self.promise-snapshot($index, :$updates);
}

method forget-snapshot($index) {
    my $promise = @!snapshot-promises[$index]:delete;
    with $promise {
        $promise.result.forget;
    }
    else {
        say "not sure why $index had no promise to forget ...";
    }
    CATCH {
        .say
    }
}

method !parse-snapshot($snapshot-task, :$updates) {
    my Concurrent::Progress $progress .= new(:1target, :!auto-done) if $updates;

    LEAVE { note "leave parse-snapshot; increment"; .increment with $progress }

    if $updates {
        start react whenever $progress {
            $updates.emit:
                %( snapshot_index => $snapshot-task<index>,
                   progress => [ .value, .target, .percent ]
               );
           say "progress: $_.value.fmt("%3d") / $_.target.fmt("%3d") - $_.percent()%";
        }
    }

    my $col-data = start {
        my int8 @col-kinds;
        my int32 @col-desc-indexes;
        my int16 @col-size;
        my int32 @col-unmanaged-size;
        my int32 @col-refs-start;
        my int32 @col-num-refs;
        my int $num-objects;
        my int $num-type-objects;
        my int $num-stables;
        my int $num-frames;
        my int $total-size;


        if $!version == 1 {
            my Channel $data .= new;
            my $split-collectables-task = start {
                $snapshot-task<collectables>.split(";").map({
                    $data.send(my uint64 @pieces = .split(",").map(*.Int));
                });
                $data.close;
            }
            while $data.receive -> @pieces {
                my int $kind = nqp::shift_i(@pieces);

                nqp::push_i(@col-kinds, $kind);

                if    $kind == 1 { $num-objects++ }
                elsif $kind == 2 { $num-type-objects++ }
                elsif $kind == 3 { $num-stables++ }
                elsif $kind == 4 { $num-frames++ }

                nqp::push_i(@col-desc-indexes, nqp::shift_i(@pieces));

                my int $size = nqp::shift_i(@pieces);
                nqp::push_i(@col-size, $size);
                my int $unmanaged-size = nqp::shift_i(@pieces);
                nqp::push_i(@col-unmanaged-size, $unmanaged-size);
                $total-size += $size + $unmanaged-size;

                nqp::push_i(@col-refs-start, nqp::shift_i(@pieces));
                nqp::push_i(@col-num-refs, nqp::shift_i(@pieces));
            }
        }
        elsif $!version == 2 {
            my $fh := MyLittleBuffer.new(fh => $snapshot-task<file>.open(:r, :bin, :buffer(4096)));
            $fh.seek($snapshot-task<collpos>, SeekFromBeginning);
            expect-header($fh, "collectables");
            my ($count, $size-per-collectable) = readSizedInt64($fh.gimme(8)) xx 2;

            $updates.emit({ index => $snapshot-task<index>, collectable-count => $count }) if $updates;

            my $done = 0;

            start react {
                whenever Supply.interval(5) {
                    last if $done;
                    $updates.emit({ index => $snapshot-task<index>, collectable-progress => $num-objects / $count }) if $updates;
                }
            }

            my $startpos = $snapshot-task<collpos> + 4 + 16;
            my $first-half-count = $count div 2;

            my $second-fh := MyLittleBuffer.new(fh => $snapshot-task<file>.open(:r, :bin, :buffer(4096)));
            $second-fh.seek($startpos + $first-half-count * $size-per-collectable, SeekFromBeginning);

            my Channel $results .= new;
            my Channel $empty-frames .= new;

            for ^5 {
                my uint64 @frame;
                $empty-frames.send(@frame)
            }

            my $first-half-done = start {
                my $start = now;
                my @result := $empty-frames.receive;
                for ^$first-half-count {
                    my @buf := $fh.gimme(2 + 4 + 2 + 8 + 8 + 4);
                    nqp::push_i(@result, readSizedInt16(@buf));
                    nqp::push_i(@result, readSizedInt32(@buf));
                    nqp::push_i(@result, readSizedInt16(@buf));
                    nqp::push_i(@result, readSizedInt64(@buf));
                    nqp::push_i(@result, readSizedInt64(@buf));
                    nqp::push_i(@result, readSizedInt32(@buf));
                    if @result.elems == 6 * 50000 {
                        $results.send(@result);
                        @result := $empty-frames.receive;
                        $start = now;
                    }
                }
                $results.send(@result);
                CATCH {
                    .say
                }
                $fh.close;
                True;
            }
            start {
                my uint64 @second-half-results;
                for ^($count - $first-half-count) {
                    my @buf := $second-fh.gimme(2 + 4 + 2 + 8 + 8 + 4);
                    nqp::push_i(@second-half-results, readSizedInt16(@buf)),
                    nqp::push_i(@second-half-results, readSizedInt32(@buf)),
                    nqp::push_i(@second-half-results, readSizedInt16(@buf)),
                    nqp::push_i(@second-half-results, readSizedInt64(@buf)),
                    nqp::push_i(@second-half-results, readSizedInt64(@buf)),
                    nqp::push_i(@second-half-results, readSizedInt32(@buf));
                }
                await $first-half-done;
                $results.send(@second-half-results);
                $results.close;
                $second-fh.close;
                True;
            }

            for $results.List -> @pieces {
                while @pieces.elems {
                    my int $kind = nqp::shift_i(@pieces);

                    nqp::push_i(@col-kinds, $kind);

                    if    $kind == 1 { $num-objects++ }
                    elsif $kind == 2 { $num-type-objects++ }
                    elsif $kind == 3 { $num-stables++ }
                    elsif $kind == 4 { $num-frames++ }

                    my int64 $desc-idx = nqp::shift_i(@pieces);
                    nqp::push_i(@col-desc-indexes, $desc-idx);

                    my int $size = nqp::shift_i(@pieces);
                    nqp::push_i(@col-size, $size);
                    my int $unmanaged-size = nqp::shift_i(@pieces);
                    nqp::push_i(@col-unmanaged-size, $unmanaged-size);
                    $total-size += $size + $unmanaged-size;

                    nqp::push_i(@col-refs-start, nqp::shift_i(@pieces));
                    nqp::push_i(@col-num-refs, nqp::shift_i(@pieces));
                }
                $empty-frames.send(@pieces);
            }
            $done = 1;
        }
        elsif $!version == 3 {
            await Promise.in(0.1);
            $snapshot-task<parser>.fetch-collectable-data(
                    toc => $snapshot-task<toc>,
                    index => $snapshot-task<index>,

                    :@col-kinds, :@col-desc-indexes, :@col-size, :@col-unmanaged-size,
                    :@col-refs-start, :@col-num-refs, :$num-objects, :$num-type-objects,
                    :$num-stables, :$num-frames, :$total-size

                    :$progress
                    );
        }

        $updates.emit({ index => $snapshot-task<index>, collectable-progress => 1 }) if $updates;

        hash(
            :@col-kinds, :@col-desc-indexes, :@col-size, :@col-unmanaged-size,
            :@col-refs-start, :@col-num-refs, :$num-objects, :$num-type-objects,
            :$num-stables, :$num-frames, :$total-size
        )
    }

    my $ref-data = start {
        my int8 @ref-kinds;
        my int32 @ref-indexes;
        my int32 @ref-tos;

        if $!version == 1 {
            for $snapshot-task<references>.split(";") {
                my int @pieces = .split(",").map(*.Int);
                @ref-kinds.push(@pieces.shift);
                @ref-indexes.push(@pieces.shift);
                @ref-tos.push(@pieces.shift);
            }
        }
        elsif $!version == 2 {
            sub grab_n_refs_starting_at($n, $pos, \ref-kinds, \ref-indexes, \ref-tos) {
                my $fh := MyLittleBuffer.new(fh => $snapshot-task<file>.open(:r, :bin, :buffer(4096)));
                $fh.seek($pos, SeekFromBeginning);

                my int $size = $fh.exactly(1)[0];

                for ^$n {
                    my @buf;
                    if $size == 54 { # "6"
                        @buf := $fh.gimme(18);
                        nqp::push_i(ref-kinds, nqp::shift_i(@buf));
                        nqp::push_i(ref-indexes, readSizedInt64(@buf));
                        nqp::push_i(ref-tos, readSizedInt64(@buf));
                        $size = nqp::shift_i(@buf);
                    }
                    elsif $size == 51 { # "3"
                        @buf := $fh.gimme(10);
                        nqp::push_i(ref-kinds, nqp::shift_i(@buf));
                        nqp::push_i(ref-indexes, readSizedInt32(@buf));
                        nqp::push_i(ref-tos, readSizedInt32(@buf));
                        $size = nqp::shift_i(@buf);
                    }
                    elsif $size == 49 { # "1"
                        @buf := $fh.gimme(6);
                        nqp::push_i(ref-kinds, nqp::shift_i(@buf));
                        nqp::push_i(ref-indexes, readSizedInt16(@buf));
                        nqp::push_i(ref-tos, readSizedInt16(@buf));
                        $size = nqp::shift_i(@buf);
                    }
                    elsif $size == 48 { # "0"
                        @buf := $fh.gimme(4);
                        nqp::push_i(ref-kinds, nqp::shift_i(@buf));
                        nqp::push_i(ref-indexes, nqp::shift_i(@buf));
                        nqp::push_i(ref-tos, nqp::shift_i(@buf));
                        $size = nqp::shift_i(@buf);
                    }
                    else {
                        die "unexpected size indicator in references blob: $size ($size.chr())";
                    }
                }
            }
            my $fh := MyLittleBuffer.new(fh => $snapshot-task<file>.open(:r, :bin, :buffer(4096)));
            $fh.seek($snapshot-task<refspos>, SeekFromBeginning);
            expect-header($fh, "references", "refs");
            my ($count, $size-per-reference) = readSizedInt64($fh.gimme(8)) xx 2;
            $fh.fh.close;

            $updates.emit({ index => $snapshot-task<index>, references-count => $count }) if $updates;

            my int8 @ref-kinds-second;
            my int32 @ref-indexes-second;
            my int32 @ref-tos-second;
            await start {
                    grab_n_refs_starting_at(
                        $count div 2,
                        $snapshot-task<refspos> + 4 + 16,
                        @ref-kinds, @ref-indexes, @ref-tos);
                },
                start {
                    grab_n_refs_starting_at(
                        $count - $count div 2,
                        $snapshot-task<halfrefpos>,
                        @ref-kinds-second, @ref-indexes-second, @ref-tos-second);
                };
            #await start { nqp::splice(@ref-kinds, @ref-kinds-second, nqp::elems(@ref-kinds), 0); },
                  #start { nqp::splice(@ref-indexes, @ref-indexes-second, nqp::elems(@ref-indexes), 0); },
                  #start { nqp::splice(@ref-tos, @ref-tos-second, nqp::elems(@ref-tos), 0); };
            $updates.emit({ index => $snapshot-task<index>, reference-progress => 0.5 }) if $updates;
            await start { @ref-kinds.append(@ref-kinds-second); },
                  start { @ref-indexes.append(@ref-indexes-second); },
                  start { @ref-tos.append(@ref-tos-second); };
            $updates.emit({ index => $snapshot-task<index>, reference-progress => 1 }) if $updates;
        }
        elsif $!version == 3 {
            $snapshot-task<parser>.fetch-references-data(
                    toc => $snapshot-task<toc>,
                    index => $snapshot-task<index>,

                    :@ref-kinds, :@ref-indexes, :@ref-tos

                    :$progress
                    );
        }
        hash(:@ref-kinds, :@ref-indexes, :@ref-tos)
    }

    Promise.allof($col-data, $ref-data, $!strings-promise, $!types-promise, $!static-frames-promise).then({ $updates.done });

    with $progress {
        note "add 5 targets for promises at end of parse-snapshot";
        .add-target(5);
        for $!strings-promise, $!types-promise, $!static-frames-promise, $col-data, $ref-data {
            dd $_, .status, so $_;
            .then({ note "one of the promises at the end of parse-snapshot; increment"; $progress.increment })
        }

    }

    Snapshot.new(
        |(await $col-data),
        |(await $ref-data),
        strings => await($!strings-promise),
        types => await($!types-promise),
        static-frames => await($!static-frames-promise)
    )
}
