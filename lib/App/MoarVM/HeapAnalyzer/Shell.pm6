use App::MoarVM::HeapAnalyzer::Model;

unit class App::MoarVM::HeapAnalyzer::Shell;


has $.model;

method interactive(IO::Path $file) {
    whine("No such file '$file'") unless $file.e;
    
    #my $*SCHEDULER = CurrentThreadScheduler.new();


    #my $*TOKEN-POOL = Channel.new;
    #$*TOKEN-POOL.send(True) xx 1;

    print "Considering the snapshot...";
    $*OUT.flush;
    #try {
        $!model = App::MoarVM::HeapAnalyzer::Model.new(:$file);
#        CATCH {
#            say "oops!\n";
#            whine(.message);
#        }
    #}
    say "looks reasonable!\n";

    my $current-snapshot;
    given $!model.num-snapshots {
        when 0 {
            whine "This file contains no heap snapshots.";
        }
        when 1 {
            say "This file contains 1 heap snapshot. I've selected it for you.";
            $current-snapshot = 0;
            $!model.prepare-snapshot($current-snapshot);
        }
        default {
            say "This file contains $_ heap snapshots. To select one to look\n"
              ~ "at, type something like `snapshot {^$_ .pick}`.";
        }
    }
    say "Type `help` for available commands, or `exit` to exit.\n";

    my &more = -> $count { say "Please run a suitable command before asking for more output" }

    if $!model.summaries -> $_ {
        my @headers =    "Snapshot", "GC Seq Num", "Heap Size", "Objects", "Type Objects", "STables", "Frames", "References";
        my @formatters = Any,        &mag,          &size,       &mag,      &mag,           &mag,      &mag,      &mag;
        my @columns = @headers Z=> @formatters;
        my @rows = .map({ flat $++, .<gc_seq_num total_heap_size total_objects total_typeobjects total_stables total_frames total_refs> });
        say table @rows, @columns;
    }

    loop {
        sub with-current-snapshot(&code) {
            without $current-snapshot {
                die "Please select a snapshot to use this instruction (`snapshot <n>`)";
            }
            if $!model.prepare-snapshot($current-snapshot) == SnapshotStatus::Preparing {
                say "Wait a moment, while I finish loading the snapshot...\n";
            }
            code($!model.get-snapshot($current-snapshot))
        }

        my constant %kind-map = hash
            objects => CollectableKind::Object,
            stables => CollectableKind::STable,
            frames  => CollectableKind::Frame;

        given prompt "> " {
            when Nil {
                last
            }
            when /^ \s* snapshot \s+ (\d+) \s* $/ {
                $current-snapshot = $0.Int;
                if $!model.prepare-snapshot($current-snapshot) == SnapshotStatus::Preparing {
                    say "Loading that snapshot. Carry on..."
                }
                else {
                    say "Snapshot loaded and ready."
                }
            }
            when /^ \s* forget \s+ snapshot \s+ (\d+) \s* $/ {
                say "forgetting snapshot $0";
                $!model.forget-snapshot($0.Int);
            }
            when 'summary' {
                with-current-snapshot -> $s {
                    say qq:to/SUMMARY/;
                        Total heap size:              &size($s.total-size)

                        Total objects:                &mag($s.num-objects)
                        Total type objects:           &mag($s.num-type-objects)
                        Total STables (type tables):  &mag($s.num-stables)
                        Total frames:                 &mag($s.num-frames)
                        Total references:             &mag($s.num-references)
                    SUMMARY
                }
            }
            when /^ summary \s+ [all | every \s+ (\d+)] $/ {
                my @headers =    "Snapshot", "Heap Size", "Objects", "Type Objects", "STables", "Frames", "References";
                my $step = ($0 || 1).Int;
                my @formatters = Any,         &size,       &mag,      &mag,           &mag,      &mag,      &mag;
                my @columns = @headers Z=> @formatters;
                my @rows = Any xx $!model.num-snapshots;
                my Supplier::Preserving $updates-supplier .= new;
                (0, $step ...^ * >= $!model.num-snapshots).hyper(:1batch, :2degree).map(-> $index {
                    my $s = await $!model.promise-snapshot($index, updates => $updates-supplier);
                    @rows[$index] = [$index, $s.total-size, $s.num-objects, $s.num-type-objects, $s.num-stables, $s.num-frames, $s.num-references];
                    $!model.forget-snapshot($index);
                    CATCH {
                        .note
                    }
                    $index;
                }).rotor(5, :partial).map(-> @indices {
                    say table @rows[@indices], @columns;
                })
            }
            when /^ top \s+ [(\d+)\s+]?
                    (< objects stables frames >)
                    [\s+ 'by' \s+ (< size count >)]? \s* 
                    $/ {
                my $n = $0 ?? $0.Int !! 15;
                my $what = ~$1;
                my $by = $2 ?? ~$2 !! 'size';
                with-current-snapshot -> $s {
                    say table
                        $s."top-by-$by"($n, %kind-map{$what}),
                        $by eq 'count'
                            ?? [ Name => Any, Count => &mag ]
                            !! [ Name => Any, 'Total Bytes' => &size ]
                }
            }
            when /^ find \s+ [(\d+)\s+]? (< objects stables frames >) \s+
                    (< type repr name >) \s* '=' \s* \" ~ \" (<-["]>+) \s*
                    $ / {
                my $n = $0 ?? $0.Int !! 15;
                my ($what, $cond, $value) = ~$1, ~$2, ~$3;
                with-current-snapshot -> $s {
                    my $result = $s.find($n, %kind-map{$what}, $cond, $value);
                    say table $result;
                    &more = -> $count = $n {
                        $result.fetch-more.($count);
                        say table $result;
                    }
                }
            }
            when /^ count \s+ (< objects stables frames >) \s+
                    (< type repr name >) \s* '=' \s* \" ~ \" (<-["]>+) \s*
                    $ / {
                my ($what, $cond, $value) = ~$0, ~$1, ~$2;
                with-current-snapshot -> $s {
                    say +$s.find(0xFFFFFFFF, %kind-map{$what}, $cond, $value);
                }
            }
            when /^ path \s+ (\d+) \s* $/ {
                my $idx = $0.Int;
                with-current-snapshot -> $s {
                    my @path = $s.path($idx);
                    my @pieces = @path.shift();
                    for @path -> $route, $target {
                        @pieces.push("    --[ $route ]-->");
                        @pieces.push($target)
                    }
                    say @pieces.join("\n") ~ "\n";
                }
            }
            when /^ show \s+ (\d+) \s* $/ {
                my $idx = $0.Int;
                with-current-snapshot -> $s {
                    my @parts = $s.details($idx);
                    my @pieces;
                    @pieces.push: @parts.shift;
                    for @parts -> $ref, $target {
                        @pieces.push("    --[ $ref ]-->");
                        @pieces.push("      $target")
                    }
                    say @pieces.join("\n") ~ "\n";
                }
            }
            when /^ incidents \s+ (\d+) \s* $/ {
                my $idx = $0.Int;
                with-current-snapshot -> $s {
                    .say for $s.reverse-refs($idx);
                }
            }
            when /^ more $/ {
                &more()
            }
            when 'help' {
                say help();
            }
            when 'exit' {
                exit 0;
            }
            default {
                say "Sorry, I don't understand.";
            }
        }
        CATCH {
                say "Oops: " ~ .message;
        }
    }
}

sub size($n) {
    mag($n) ~ ' bytes'
}

sub mag($n) {
    $n.Str.flip.comb(3).join(',').flip
}

sub formatter-for-unit($_) {
    when Count { &mag }
    when Bytes { &size }
    default { Any }
}

sub table($inc-data, @inc-columns?) {
    my @data;
    my @columns;

    if $inc-data ~~ App::MoarVM::HeapAnalyzer::Model::Result {
        @data = $inc-data.values.skip($inc-data.batch-starts-at);
        @columns = $inc-data.headers Z=> $inc-data.units.map(&formatter-for-unit)
    } else {
        @data = @$inc-data;
        @columns = @inc-columns;
    }

    my @formatters = @columns>>.value;
    my @formatted-data = @data.map(-> @row {
        list @row.pairs.map({
            @formatters[.key] ~~ Callable
                ?? @formatters[.key](.value)
                !! .value
        })
    });

    my @names = @columns>>.key;
    my @col-widths = ^@columns
        .map({ (flat $@names, @formatted-data)>>.[$_]>>.chars.max });

    my @pieces;
    for ^@columns -> $i {
        push @pieces, @names[$i];
        push @pieces, ' ' x 2 + @col-widths[$i] - @names[$i].chars;
    }
    push @pieces, "\n";
    for ^@columns -> $i {
        push @pieces, '=' x @col-widths[$i];
        push @pieces, "  ";
    }
    push @pieces, "\n";
    for @formatted-data -> @row {
        for ^@columns -> $i {
            push @pieces, @row[$i];
            push @pieces, ' ' x 2 + @col-widths[$i] - @row[$i].chars;
        }
        push @pieces, "\n";
    }
    @pieces.join("")
}

sub help() {
    q:to/HELP/
    General:
        snapshot <n>
            Work with snapshot <n>
        exit
            Exit this application
    
    On the currently selected snapshot:
        summary
            Basic summary information
        top [<n>]? <what> [by size | by count]?
            Where <what> is objects, stables, or frames. By default, <n> is 15
            and they are ordered by their total memory size.
        find [<n>]? <what> [type="..." | repr="..." | name="..."]
            Where <what> is objects, stables, or frames. By default, <n> is 15.
            Finds items matching the given type or REPR, or frames by name.
        count <what> [type="..." | repr="..." | name="..."]
            Where <what> is objects, stables, or frames. Counts the number of
            items matching the given type or REPR, or frames by name.
        path <objectid>
            Shortest path from the root to <objectid> (find these with `find`)
        show <objectid>
            Shows more information about <objectid> as well as all outgoing
            references.
        more
            Displays more results, if possible.
    HELP
}

sub whine ($msg) {
    note $msg;
    exit 1;
}
