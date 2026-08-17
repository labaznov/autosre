//! Правило отклонения: робастный z-score по медиане и MAD.
//!
//! Среднее и стандартное отклонение ломаются об собственные выбросы: один пик
//! поднимает базовую линию так, что следующий такой же считается нормой.
//! Медиана и медианное абсолютное отклонение к выбросам нечувствительны — а у
//! нас вся работа как раз про выбросы.

/// Пороги срабатывания, свои у каждого горизонта.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Thresholds {
    /// Ниже этого значения окно не рассматривается ни при какой статистике.
    pub minimum: f64,
    /// Наименьший робастный z-score.
    pub score: f64,
    /// Во сколько раз окно должно превышать базовую линию.
    pub ratio: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            minimum: 20.0,
            score: 3.5,
            ratio: 2.0,
        }
    }
}

/// Приговор по одному окну вместе с числами, которые к нему привели.
///
/// Числа хранятся целиком, а не выбрасываются после сравнения: дежурный имеет
/// право спросить «почему», и ответ «91 против обычных 4,5» — это ответ, а
/// «сработал порог» — нет.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Verdict {
    pub deviates: bool,
    pub value: f64,
    pub baseline: f64,
    pub spread: f64,
    pub score: f64,
    pub weight: f64,
}

/// Предел, выше которого рост оценки уже не влияет на вес.
///
/// Ровная базовая линия даёт бесконечную оценку, и без предела вес любого
/// отклонения от тишины оказался бы больше веса настоящей аварии.
const CAP: f64 = 50.0;

/// Детектор с фиксированными порогами.
#[derive(Debug, Clone, Copy, Default)]
pub struct Detector {
    thresholds: Thresholds,
}

impl Detector {
    #[must_use]
    pub fn new(thresholds: Thresholds) -> Self {
        Self { thresholds }
    }

    /// Сравнивает окно с его историей.
    ///
    /// Пустой разброс (ровный ряд) даёт бесконечную оценку, поэтому решение в
    /// этом случае удерживают `minimum` и `ratio`.
    #[must_use]
    pub fn verdict(&self, history: &[f64], value: f64) -> Verdict {
        let baseline = median(history);
        let spread = spread(history, baseline);
        let score = if spread > 0.0 {
            0.6745 * (value - baseline) / spread
        } else if value > baseline {
            f64::INFINITY
        } else {
            0.0
        };
        Verdict {
            deviates: value >= self.thresholds.minimum
                && score >= self.thresholds.score
                && value >= self.thresholds.ratio * baseline,
            value,
            baseline,
            spread,
            score,
            weight: weight(value, baseline, score),
        }
    }
}

/// Вес отклонения: чем тяжелее, тем раньше его разберут.
///
/// Складывается из двух вещей, которые дежурный назвал бы сам: насколько много
/// (превышение над обычным) и насколько необычно (оценка). Ни одна из них по
/// отдельности не годится: тысяча ошибок при обычных девятистах — не беда, а
/// двадцать при обычных нуле — беда.
///
/// Дальше сюда добавятся охват, известность сигнатуры и важность сервиса
/// ([ADR-0018](../../../docs/adr/0018-investigation-queue.md)).
#[must_use]
pub fn weight(value: f64, baseline: f64, score: f64) -> f64 {
    let excess = (value - baseline).max(0.0);
    excess * (1.0 + score.min(CAP)).ln()
}

/// Медиана ряда; пустой ряд считается нулевым.
#[must_use]
pub fn median(series: &[f64]) -> f64 {
    if series.is_empty() {
        return 0.0;
    }
    let mut sorted = series.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        f64::midpoint(sorted[middle - 1], sorted[middle])
    } else {
        sorted[middle]
    }
}

/// Медианное абсолютное отклонение ряда от его медианы.
fn spread(series: &[f64], middle: f64) -> f64 {
    if series.is_empty() {
        return 0.0;
    }
    let apart: Vec<f64> = series.iter().map(|value| (value - middle).abs()).collect();
    median(&apart)
}
