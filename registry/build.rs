use protobuf_codegen::Codegen;
use protobuf_codegen::Customize;

fn main() {
    std::fs::create_dir_all("src/protos").unwrap();
    Codegen::new()
        .pure()
        .customize(Customize::default().gen_mod_rs(true))
        .out_dir("src/protos")
        .inputs(&["protos/registry.proto"])
        .include("protos")
        .run_from_script();
}
