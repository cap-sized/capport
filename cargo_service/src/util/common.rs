use yaml_rust2::Yaml;

pub trait SvcDefault {
    fn default_with_svc() -> Self;
    fn default_with_svc_config(args: &Yaml) -> Self;
}
