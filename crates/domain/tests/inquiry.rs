use autosre_domain::Inquiry;

#[test]
fn takes_a_command_that_only_looks() {
    assert!(Inquiry::new("db-01", "ss -tnp state established", "кто держит порты").is_ok());
}

#[test]
fn keeps_the_command_as_it_was_offered() {
    let inquiry = Inquiry::new("db-01", "  df -h /var  ", "место на диске").unwrap();
    assert_eq!(inquiry.command, "df -h /var");
}

#[test]
fn refuses_to_delete_anything() {
    assert!(Inquiry::new("db-01", "rm -rf /var/log/old", "освободить место").is_err());
}

#[test]
fn refuses_a_restart_hidden_behind_a_reader() {
    assert!(Inquiry::new("db-01", "systemctl restart nginx", "поднять").is_err());
}

#[test]
fn lets_a_status_of_the_same_unit_through() {
    assert!(Inquiry::new("db-01", "systemctl status nginx", "жив ли").is_ok());
}

#[test]
fn refuses_a_second_command_chained_after_a_safe_one() {
    assert!(Inquiry::new("db-01", "df -h; kill -9 1234", "место").is_err());
}

#[test]
fn refuses_to_write_a_file() {
    assert!(Inquiry::new("db-01", "cat /proc/meminfo > /tmp/mem", "память").is_err());
}

#[test]
fn refuses_a_full_path_to_a_dangerous_binary() {
    assert!(Inquiry::new("db-01", "/bin/rm /var/lib/pgsql/x", "место").is_err());
}

#[test]
fn refuses_an_empty_command() {
    assert!(Inquiry::new("db-01", "   ", "непонятно зачем").is_err());
}

#[test]
fn names_the_command_it_refused() {
    let refusal = Inquiry::new("db-01", "rm -rf /", "").unwrap_err();
    assert!(refusal.to_string().contains("rm -rf /"));
}
