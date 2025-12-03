#define DUCKDB_EXTENSION_MAIN

#include "read_rdf_extension.hpp"
#include "duckdb.hpp"
#include "include/serd_buffer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "duckdb/common/file_system.hpp"
#include <mutex>

using namespace std;

namespace duckdb {

struct RDFReaderBindData : public TableFunctionData {
	vector<string> files;
	string baseURI;
};

struct RDFReaderGlobalState : public GlobalTableFunctionState {
	mutex file_lock;
	idx_t next_file_index = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct RDFReaderLocalState : public LocalTableFunctionState {
	unique_ptr<SerdBuffer> sb;
	string current_file;
	bool finished = false;
};

// Helper to extract basename from path
static string GetBasename(const string &path) {
	auto pos = path.find_last_of("/\\");
	if (pos == string::npos) {
		return path;
	}
	return path.substr(pos + 1);
}

static unique_ptr<FunctionData> RDFReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RDFReaderBindData>();
	string file_pattern = input.inputs[0].GetValue<string>();

	// Handle optional base_uri parameter (named parameter)
	auto base_uri_param = input.named_parameters.find("base_uri");
	if (base_uri_param != input.named_parameters.end() && !base_uri_param->second.IsNull()) {
		result->baseURI = base_uri_param->second.GetValue<string>();
	}

	// Expand glob pattern to get list of files
	auto &fs = FileSystem::GetFileSystem(context);
	auto glob_result = fs.GlobFiles(file_pattern, context);

	// Extract file paths from OpenFileInfo results
	for (auto &file_info : glob_result) {
		result->files.push_back(file_info.path);
	}

	// Sort files for deterministic ordering
	std::sort(result->files.begin(), result->files.end());

	// Output schema: filename first, then RDF columns
	names = {"filename", "graph", "subject", "predicate", "object", "object_datatype", "object_lang"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RDFReaderGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RDFReaderGlobalState>();
}
// Helper to get next file from global state
static bool TryGetNextFile(RDFReaderGlobalState &gstate, const RDFReaderBindData &bind_data, string &out_file) {
	lock_guard<mutex> lock(gstate.file_lock);
	if (gstate.next_file_index >= bind_data.files.size()) {
		return false;
	}
	out_file = bind_data.files[gstate.next_file_index++];
	return true;
}

// Helper to open a file and start parsing
static bool OpenNextFile(RDFReaderLocalState &lstate, RDFReaderGlobalState &gstate,
                         const RDFReaderBindData &bind_data) {
	string file_path;
	if (!TryGetNextFile(gstate, bind_data, file_path)) {
		lstate.finished = true;
		return false;
	}

	lstate.current_file = GetBasename(file_path);
	auto sb = make_uniq<SerdBuffer>(file_path, bind_data.baseURI);
	sb->StartParse();
	lstate.sb = std::move(sb);
	return true;
}

static unique_ptr<LocalTableFunctionState> RDFReaderInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<RDFReaderBindData>();
	auto &gstate = global_state->Cast<RDFReaderGlobalState>();
	auto state = make_uniq<RDFReaderLocalState>();

	// No files matched the glob pattern
	if (bind_data.files.empty()) {
		state->finished = true;
		return state;
	}

	// Open the first file
	try {
		OpenNextFile(*state, gstate, bind_data);
	} catch (const std::runtime_error &re) {
		throw IOException(re.what());
	}
	return state;
}

static void RDFReaderFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &lstate = input.local_state->Cast<RDFReaderLocalState>();
	auto &gstate = input.global_state->Cast<RDFReaderGlobalState>();
	auto &bind_data = input.bind_data->Cast<RDFReaderBindData>();

	// Already finished all files
	if (lstate.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	const idx_t target = STANDARD_VECTOR_SIZE;

	while (count < target) {
		try {
			// Check if current file is exhausted
			if (lstate.sb->EverythingProcessed()) {
				// Try to open next file
				if (!OpenNextFile(lstate, gstate, bind_data)) {
					break; // No more files
				}
				continue; // Retry with new file
			}

			RDFRow row = lstate.sb->GetNextRow();
			// Column 0: filename (basename)
			output.SetValue(0, count, Value(lstate.current_file));
			// Columns 1-6: RDF data
			output.SetValue(1, count, Value(row.graph));
			output.SetValue(2, count, Value(row.subject));
			output.SetValue(3, count, Value(row.predicate));
			output.SetValue(4, count, Value(row.object));
			output.SetValue(5, count, Value(row.datatype));
			output.SetValue(6, count, Value(row.lang));
			count++;
		} catch (const std::runtime_error &error) {
			throw SyntaxException(error.what());
		}
	}
	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	string extension_name = "read_rdf";
	TableFunction tf(extension_name, {LogicalType::VARCHAR}, RDFReaderFunc, RDFReaderBind, RDFReaderGlobalInit,
	                 RDFReaderInit);
	// Register optional named parameter for base URI
	tf.named_parameters["base_uri"] = LogicalType::VARCHAR;
	loader.RegisterFunction(tf);
}

void ReadRdfExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string ReadRdfExtension::Name() {
	return "read_rdf";
}

std::string ReadRdfExtension::Version() const {
#ifdef EXT_VERSION_READ_RDF
	return EXT_VERSION_READ_RDF;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(read_rdf, loader) {
	duckdb::LoadInternal(loader);
}
/*DUCKDB_EXTENSION_API void read_rdf_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ReadRdfExtension>();
}

DUCKDB_EXTENSION_API const char *read_rdf_version() {
    return duckdb::DuckDB::LibraryVersion();
}*/
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
