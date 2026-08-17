//! Правило отклонения: робастный z-score по медиане и MAD.
//!
//! Среднее и стандартное отклонение ломаются об собственные выбросы: один пик
//! поднимает базовую линию так, что следующий такой же считается нормой.
//! Медиана и медианное абсолютное отклонение к выбросам нечувствительны — а у
//! нас вся работа как раз про выбросы.

use crate::bucket::Kind;

/// Пороги срабатывания, свои у каждого горизонта.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Thresholds {
    /// Ниже этого значения окно не рассматривается ни при какой статистике.
    pub minimum: f64,
    /// Наименьший робастный z-score.
    pub score: f64,
    /// Во сколько раз окно должно превышать базовую линию. Для счётчиков.
    pub ratio: f64,
    /// На какую долю должен сдвинуться уровень. Для метрик-уровней.
    ///
    /// Отношение здесь не годится: память, выросшая на четверть, — беда, а
    /// требование вырасти вдвое означает, что мы заметим её, когда сервис уже
    /// умрёт.
    pub drift: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            minimum: 20.0,
            score: 3.5,
            ratio: 2.0,
            drift: 0.15,
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

/// Какая доля шагов должна идти в одну сторону, чтобы движение считалось
/// стойким, а не дрожанием.
const STEADY: f64 = 0.7;

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
    /// Правил два, потому что сигналы разные. У счётчика важно, во сколько раз
    /// стало больше: двадцать ошибок при обычных двух — беда. У уровня важно,
    /// куда он поехал: память, выросшая на четверть и не вернувшаяся, — беда,
    /// хотя «во сколько раз» тут смешная величина.
    ///
    /// Пустой разброс (ровный ряд) даёт бесконечную оценку, поэтому решение в
    /// этом случае удерживают пороги величины, а не статистика.
    #[must_use]
    pub fn verdict(&self, history: &[f64], value: f64, kind: Kind) -> Verdict {
        let baseline = median(history);
        let spread = spread(history, baseline);
        // Ровная история и любое отличие — оценка бесконечна, но со знаком:
        // иначе падение уровня при неподвижной базовой линии выглядит как
        // «ничего не произошло», а это ровно случай убывающего диска.
        let score = if spread > 0.0 {
            0.6745 * (value - baseline) / spread
        } else if value > baseline {
            f64::INFINITY
        } else if value < baseline {
            f64::NEG_INFINITY
        } else {
            0.0
        };
        let deviates = match kind {
            // У счётчика важен рост: никого не будят оттого, что ошибок стало
            // меньше.
            Kind::Sum => {
                value >= self.thresholds.minimum
                    && value >= self.thresholds.ratio * baseline
                    && score >= self.thresholds.score
            }
            // У уровня важны обе стороны и, главное, направление. Ровный тренд
            // робастная оценка не видит по построению: базовая линия ползёт
            // вместе с ним, и разброс выходит соразмерным шагу. Поэтому уровень
            // сравнивается со старшей половиной истории, а подтверждением
            // служит либо стойкость движения, либо всё-таки скачок.
            Kind::Mean => {
                let older = median(&history[history.len() / 2..]);
                drift(value, older) >= self.thresholds.drift
                    && (steady(history, value) >= STEADY || score.abs() >= self.thresholds.score)
            }
        };
        Verdict {
            deviates,
            value,
            baseline,
            spread,
            score,
            weight: weight(value, baseline, score, kind),
        }
    }
}

/// Доля шагов ряда, идущих в сторону общего движения.
///
/// История приходит от свежего к старому, поэтому разворачивается: стойким
/// считается движение, а не порядок чтения.
fn steady(history: &[f64], value: f64) -> f64 {
    let mut walk: Vec<f64> = history.iter().rev().copied().collect();
    walk.push(value);
    if walk.len() < 3 {
        return 0.0;
    }
    let total = value - walk[0];
    if total.abs() <= f64::EPSILON {
        return 0.0;
    }
    let same = walk
        .windows(2)
        .filter(|pair| (pair[1] - pair[0]) * total > 0.0)
        .count();
    ratio(same) / ratio(walk.len() - 1)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "число окон истории далеко ниже предела точности f64"
)]
fn ratio(count: usize) -> f64 {
    count as f64
}

/// Насколько уровень отошёл от обычного, в долях от обычного.
fn drift(value: f64, baseline: f64) -> f64 {
    if baseline.abs() > f64::EPSILON {
        (value - baseline).abs() / baseline.abs()
    } else if value.abs() > f64::EPSILON {
        f64::INFINITY
    } else {
        0.0
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
/// Для уровня превышением считается сдвиг в любую сторону: диск, потерявший
/// половину свободного места, весит столько же, сколько память, набравшая
/// столько же.
#[must_use]
pub fn weight(value: f64, baseline: f64, score: f64, kind: Kind) -> f64 {
    let excess = match kind {
        Kind::Sum => (value - baseline).max(0.0),
        Kind::Mean => (value - baseline).abs(),
    };
    excess * (1.0 + score.abs().min(CAP)).ln()
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
