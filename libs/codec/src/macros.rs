macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
}

macro_rules! prop_len {
    ($value:expr, $len:expr) => {
        if $value.is_some() {
            1 + $len
        } else {
            0
        }
    };
}

macro_rules! prop_remaining_length_len {
    ($value:expr) => {
        if let Some(value) = $value {
            1 + $crate::writer::bytes_remaining_length(value)?
        } else {
            0
        }
    };
}

macro_rules! prop_data_len {
    ($value:expr) => {
        if let Some(value) = &$value {
            1 + 2 + value.len()
        } else {
            0
        }
    };
}

macro_rules! prop_kv_len {
    ($key:expr, $value:expr) => {
        1 + 2 + $key.len() + 2 + $value.len()
    };
}
