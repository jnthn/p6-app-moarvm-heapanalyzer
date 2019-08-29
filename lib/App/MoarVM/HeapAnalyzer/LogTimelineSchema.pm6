unit module App::MoarVM::HeapAnalyzer::Log;
use Log::Timeline;

class ParseTOCs does Log::Timeline::Task['HeapAnalyzer', 'Parser', 'Parse Tables of Content'] { }

class ParseTOCFound does Log::Timeline::Event["HeapAnalyzer", "Parser", "TOC found"] { }

class ParseStrings does Log::Timeline::Task['HeapAnalyzer', 'Parser', 'Parse Strings'] { }
class ParseStaticFrames does Log::Timeline::Task['HeapAnalyzer', 'Parser', 'Parse Static Frames'] { }
class ParseTypes does Log::Timeline::Task['HeapAnalyzer', 'Parser', 'Parse Static Types'] { }

class ParseAttributeStream does Log::Timeline::Task['HeapAnalyzer', 'Parser', 'Parse Attribute Stream'] { }