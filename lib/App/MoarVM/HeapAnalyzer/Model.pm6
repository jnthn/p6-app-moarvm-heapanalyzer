unit class App::MoarVM::HeapAnalyzer::Model;

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
    has int @!col-desc-indexes;
    has int16 @!col-size;
    has int @!col-unmanaged-size;
    has int @!col-refs-start;
    has int32 @!col-num-refs;

    has @!strings;
    has $!types;
    has $!static-frames;

    has $.num-objects;
    has $.num-type-objects;
    has $.num-stables;
    has $.num-frames;
    has $.total-size;

    has int8 @!ref-kinds;
    has int @!ref-indexes;
    has int @!ref-tos;

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

        my @results;
        my int $num-cols = @!col-kinds.elems;
        loop (my int $i = 0; $i < $num-cols; $i++) {
            if @!col-kinds[$i] == $kind && @matching[@!col-desc-indexes[$i]] {
                @results.push: [
                    $i,
                    $kind == CollectableKind::Frame
                        ?? $!static-frames.summary(@!col-desc-indexes[$i])
                        !! $!types.type-name(@!col-desc-indexes[$i])
                ];
                last if @results == $n;
            }
        }
        @results
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

    method !ensure-bfs() {
        return if @!bfs-distances;

        my int32 @distances;
        my int @pred;
        my int @pred-ref;
        my int8 @color; # 0 = white, 1 = grey, 2 = black

        @color[0] = 1;
        @distances[0] = 0;
        @pred[0] = -1;
        @pred-ref[0] = -1;

        my int @queue;
        @queue.push(0);
        while @queue {
            my int $cur-col = @queue.shift;
            my int $num-refs = @!col-num-refs[$cur-col];
            my int $refs-start = @!col-refs-start[$cur-col];
            loop (my int $i = 0; $i < $num-refs; $i++) {
                my int $ref-idx = $refs-start + $i;
                my int $to = @!ref-tos[$ref-idx];
                if @color[$to] == 0 {
                    @color[$to] = 1;
                    @distances[$to] = @distances[$cur-col] + 1;
                    @pred[$to] = $cur-col;
                    @pred-ref[$to] = $ref-idx;
                    @queue.push($to);
                }
            }
            @color[$cur-col] = 2;
        }

        @!bfs-distances := @distances;
        @!bfs-preds := @pred;
        @!bfs-pred-refs := @pred-ref;
    }
}

class MyLittleBuffer {
    has $!buffer = Buf.new();
    has $.fh;

    method gimme(int $size) {
        if $!buffer.elems > $size {
            $!buffer;
        } else {
            my $newbuf := $!fh.read(4096);
            if $!buffer {
                $!buffer ~= $newbuf;
            } else {
                $!buffer = $newbuf;
            }
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
        "NYI"
    }
}

sub readSizedInt64(@buf) {
    #my $bytesize = 8;
    #my @buf := $fh.gimme(8);
    #die "expected $bytesize bytes, but got { @buf.elems() }" unless @buf.elems >= $bytesize;

    my int64 $result =
            nqp::add_i nqp::shift_i(@buf),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf),  8),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 16),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 24),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 32),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 40),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 48),
                       nqp::bitshiftl_i(nqp::shift_i(@buf), 56)
}
sub readSizedInt32(@buf) {
    #my $bytesize = 4;
    #my @buf := $fh.gimme(4);
    #die "expected $bytesize bytes, but got { @buf.elems() }" unless @buf.elems >= $bytesize;

    my int64 $result =
            nqp::add_i nqp::shift_i(@buf),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf),  8),
            nqp::add_i nqp::bitshiftl_i(nqp::shift_i(@buf), 16),
                       nqp::bitshiftl_i(nqp::shift_i(@buf), 24)
}
sub readSizedInt16(@buf) {
    #my $bytesize = 2;
    #my @buf := $fh.gimme(2);
    #die "expected $bytesize bytes, but got { @buf.elems() }" unless @buf.elems >= $bytesize;

    my int64 $result =
            @buf.shift +
            @buf.shift +< 8;
}

submethod BUILD(IO::Path :$file = die "Must construct model with a file") {
    # Pull data from the file.
    my %top-level;
    my @snapshots;
    my $cur-snapshot-hash;

    $!version = 1;

    try {
        my $fh = $file.open(:r, :enc<latin1>);
        if $fh.readchars(16) eq "MoarHeapDumpv002" {
            $!version = 2;
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
        $fh.seek(-8 * index-entries, SeekFromEnd);
        my @sizes = readSizedInt64($fh.gimme(8)) xx index-entries;
        dd @sizes;
        my ($stringheap_size, $types_size, $staticframe_size, $snapshot_entry_count) = @sizes;
        @sizes.pop; # remove the number of snapshot entries

        sub fh-at($pos) {
            my $fh = MyLittleBuffer.new(fh => $file.open(:r, :bin, :buffer(4096)));
            $fh.seek($pos, SeekFromBeginning);
            $fh
        }

        my @positions = [\+] 16, $stringheap_size, $types_size, $staticframe_size;
        dd @positions;
        my @fds = @positions.map(&fh-at);
        my ($stringheap_fd, $types_fd, $staticframe_fd, $snapshots_fd) = @fds;

        $!strings-promise       = start self!parse-strings-ver2($stringheap_fd);
        $!types-promise         = start self!parse-types-ver2($types_fd);
        $!static-frames-promise = start self!parse-static-frames-ver2($staticframe_fd);

        $fh.seek(-8 * index-entries - 16 * $snapshot_entry_count, SeekFromEnd);
        my $snapshot-position = @positions.tail;
        @!unparsed-snapshots = do for ^$snapshot_entry_count {
            my @buf := $fh.gimme(16);
            my @sizes = readSizedInt64(@buf), readSizedInt64(@buf);
            my $collpos = $snapshot-position;
            my $refspos = $collpos + @sizes[0];
            $snapshot-position += @sizes[0] + @sizes[1];
            [$collpos, $refspos, $file];
        }
    }
}

method !parse-strings-ver2($fh) {
    die "expected the strings header" if $fh.exactly(4).decode("utf8") ne "strs";
    my $stringcount = readSizedInt64($fh.gimme(8));
    do for ^$stringcount {
        my $length = readSizedInt64($fh.gimme(8));
        $length ?? $fh.exactly($length).decode("utf8")
                !! ""
    }
}
method !parse-types-ver2($fh) {
    die "expected the types header" if $fh.exactly(4).decode("utf8") ne "type";
    my ($typecount, $size-per-type) = readSizedInt64($fh.gimme(8)) xx 2;
    my int @repr-name-indexes;
    my int @type-name-indexes;
    for ^$typecount {
        my @buf := $fh.gimme(24);
        my $length = readSizedInt64(@buf);
        @repr-name-indexes.push(readSizedInt64(@buf));
        @type-name-indexes.push(readSizedInt64(@buf));
    }
    Types.new(:@repr-name-indexes, :@type-name-indexes, strings => await $!strings-promise);
}
method !parse-static-frames-ver2($fh) {
    die "expected the frames header" if $fh.exactly(4).decode("utf8") ne "fram";
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

enum SnapshotStatus is export <Preparing Ready>;

method prepare-snapshot($index) {
    with @!snapshot-promises[$index] -> $prom {
        given $prom.status {
            when Kept { Ready }
            when Broken { await $prom }
            default { Preparing }
        }
    }
    else {
        with @!unparsed-snapshots[$index] {
            @!snapshot-promises[$index] = start self!parse-snapshot($_);
            Preparing
        }
        else {
            die "No such snapshot $index"
        }
    }
}

method get-snapshot($index) {
    await @!snapshot-promises[$index] //= start self!parse-snapshot(
        @!unparsed-snapshots[$index]
    )
}

method !parse-snapshot($snapshot-task) {
    my $col-data = start {
        my int8 @col-kinds;
        my int @col-desc-indexes;
        my int16 @col-size;
        my int @col-unmanaged-size;
        my int @col-refs-start;
        my int32 @col-num-refs;
        my int $num-objects;
        my int $num-type-objects;
        my int $num-stables;
        my int $num-frames;
        my int $total-size;

        my @collectable-pieces = do {
            if $!version == 1 {
                $snapshot-task<collectables>.split(";").map({
                    my uint64 @pieces = .split(",").map(*.Int);
                })
            }
            elsif $!version == 2 {
                my $fh := MyLittleBuffer.new(fh => $snapshot-task.tail.open(:r, :bin, :buffer(4096)));
                $fh.seek($snapshot-task[0], SeekFromBeginning);
                die "expected the collectables header" if $fh.exactly(4).decode("utf8") ne "coll";
                my ($count, $size-per-collectable) = readSizedInt64($fh.gimme(8)) xx 2;

                my $startpos = $snapshot-task[0] + 4 + 16;
                my $first-half-count = $count div 2;

                my $second-fh := MyLittleBuffer.new(fh => $snapshot-task.tail.open(:r, :bin, :buffer(4096)));
                $second-fh.seek($startpos + $first-half-count * $size-per-collectable, SeekFromBeginning);
                await start {
                        do for ^$first-half-count {
                            my @buf := $fh.gimme(2 + 4 + 2 + 8 + 8 + 4);
                            my uint64 @ = readSizedInt16(@buf),
                                readSizedInt32(@buf),
                                readSizedInt16(@buf),
                                readSizedInt64(@buf),
                                readSizedInt64(@buf),
                                readSizedInt32(@buf);
                        }.Slip
                    },
                    start {
                        do for ^($count - $first-half-count) {
                            my @buf := $second-fh.gimme(2 + 4 + 2 + 8 + 8 + 4);
                            my uint64 @ = readSizedInt16(@buf),
                                readSizedInt32(@buf),
                                readSizedInt16(@buf),
                                readSizedInt64(@buf),
                                readSizedInt64(@buf),
                                readSizedInt32(@buf);
                        }.Slip
                    }
            }
        }

        for @collectable-pieces -> @pieces {
            my int $kind = @pieces.shift;
            @col-kinds.push($kind);
            if    $kind == 1 { $num-objects++ }
            elsif $kind == 2 { $num-type-objects++ }
            elsif $kind == 3 { $num-stables++ }
            elsif $kind == 4 { $num-frames++ }

            @col-desc-indexes.push(@pieces.shift);

            my int $size = @pieces.shift;
            @col-size.push($size);
            my int $unmanaged-size = @pieces.shift;
            @col-unmanaged-size.push($unmanaged-size);
            $total-size += $size + $unmanaged-size;

            @col-refs-start.push(@pieces.shift);
            @col-num-refs.push(@pieces.shift);
        }
        CATCH {
            .say
        }
        hash(
            :@col-kinds, :@col-desc-indexes, :@col-size, :@col-unmanaged-size,
            :@col-refs-start, :@col-num-refs, :$num-objects, :$num-type-objects,
            :$num-stables, :$num-frames, :$total-size
        )
    }

    my $ref-data = start {
        my int8 @ref-kinds;
        my int @ref-indexes;
        my int @ref-tos;

        if $!version == 1 {
            for $snapshot-task<collectables>.split(";") {
                my uint8 @pieces = .split(",").map(*.Int);
                @ref-kinds.push(@pieces.shift);
                @ref-indexes.push(@pieces.shift);
                @ref-tos.push(@pieces.shift);
            }
        }
        elsif $!version == 2 {
            sub grab_n_refs_starting_at($n, $pos, \ref-kinds, \ref-indexes, \ref-tos) {
                my $fh := MyLittleBuffer.new(fh => $snapshot-task.tail.open(:r, :bin, :buffer(4096)));
                $fh.seek($pos, SeekFromBeginning);
                for ^$n {
                    my @buf := $fh.gimme(24);
                    ref-kinds.push(readSizedInt64(@buf));
                    ref-indexes.push(readSizedInt64(@buf));
                    ref-tos.push(readSizedInt64(@buf));
                }
            }
            my $fh := MyLittleBuffer.new(fh => $snapshot-task.tail.open(:r, :bin, :buffer(4096)));
            $fh.seek($snapshot-task[1], SeekFromBeginning);
            die "expected the references header" if $fh.exactly(4).decode("utf8") ne "refs";
            my ($count, $size-per-reference) = readSizedInt64($fh.gimme(8)) xx 2;
            $fh.fh.close;
            my int8 @ref-kinds-second;
            my int @ref-indexes-second;
            my int @ref-tos-second;
            await start {
                    grab_n_refs_starting_at(
                        $count div 2,
                        $snapshot-task[1] + 4 + 16,
                        @ref-kinds, @ref-indexes, @ref-tos);
                },
                start {
                    grab_n_refs_starting_at(
                        $count - $count div 2,
                        $snapshot-task[1] + ($count div 2) * $size-per-reference + 4 + 16,
                        @ref-kinds-second, @ref-indexes-second, @ref-tos-second);
                };
            await start { @ref-kinds.splice(+@ref-kinds, 0, @ref-kinds-second); },
                  start { @ref-indexes.splice(+@ref-indexes, 0, @ref-indexes-second); },
                  start { @ref-tos.splice(+@ref-tos, 0, @ref-tos-second); };
        }
        CATCH {
            .say
        }
        hash(:@ref-kinds, :@ref-indexes, :@ref-tos)
    }

    Snapshot.new(
        |(await $col-data),
        |(await $ref-data),
        strings => await($!strings-promise),
        types => await($!types-promise),
        static-frames => await($!static-frames-promise)
    )
}
