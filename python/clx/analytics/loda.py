import cupy as cp


class Loda:
    """
    Anomaly detection using Lightweight Online Detector of Anomalies (LODA). Loda detects anomalies in a dataset 
    by computing the likelihood of data points using an ensemble of one-dimensional histograms.
    """

    def __init__(self, n_bins=None, n_random_cuts=100):
        """
        Loda constructor
        :param n_bins: Number of bins for each histogram. If None a heuristic is used to compute the number of bins.
        :type n_bins: int
        :param n_random_cuts: Number of projection to use.
        :type n_random_cuts: int
        """
        self._n_bins = n_bins
        self._n_random_cuts = n_random_cuts
        self._weights = cp.ones(n_random_cuts) / n_random_cuts
        self._projections = None
        self._histograms = None
        self._limits = None

    def fit(self, train_data):
        """
        Fit training data and construct histogram.
        The type of histogram is 'regular', and right-open
        Note: If n_bins=None, the number of breaks is being computed as in:
        L. Birge, Y. Rozenholc, How many bins should be put in a regular
        histogram? 2006.

        :param train_data: NxD training sample
        :type train_data: cupy.ndarray
        """
        nrows, n_components = train_data.shape
        if not self._n_bins:
            self._n_bins = int(1 * (nrows ** 1) * (cp.log(nrows) ** -1))
        n_nonzero_components = cp.sqrt(n_components)
        n_zero_components = n_components - cp.int(n_nonzero_components)

        self._projections = cp.random.randn(self._n_random_cuts, n_components)
        self._histograms = cp.zeros([self._n_random_cuts, self._n_bins])
        self._limits = cp.zeros((self._n_random_cuts, self._n_bins + 1))
        for i in range(self._n_random_cuts):
            rands = cp.random.permutation(n_components)[:n_zero_components]
            self._projections[i, rands] = 0.
            projected_data = self._projections[i, :].dot(train_data.T)
            self._histograms[i, :], self._limits[i, :] = cp.histogram(
                projected_data, bins=self._n_bins, density=False)
            self._histograms[i, :] += 1e-12
            self._histograms[i, :] /= cp.sum(self._histograms[i, :])

    def score(self, input_data):
        """
        Calculate anomaly scores using negative likelihood across n_random_cuts histograms.

        :param input_data: NxD training sample
        :type input_data: cupy.ndarray
        """
        if cp.ndim(input_data) < 2:
            input_data = input_data.reshape(1, -1)
        pred_scores = cp.zeros([input_data.shape[0], 1])
        for i in range(self._n_random_cuts):
            projected_data = self._projections[i, :].dot(input_data.T)
            inds = cp.searchsorted(self._limits[i, :self._n_bins - 1],
                                   projected_data, side='left')
            pred_scores[:, 0] += -self._weights[i] * cp.log(
                self._histograms[i, inds])
        pred_scores /= self._n_random_cuts
        return pred_scores.ravel()

    
    def explain(self, anomaly, scaled=True):
        """
        Explain anomaly based on contributions (t-scores) of each feature across histograms.

        :param anomaly: selected anomaly from input dataset
        :type anomaly: cupy.ndarray
        """
        if cp.ndim(anomaly) < 2:
            anomaly = anomaly.reshape(1, -1)
        ranked_feature_importance = cp.zeros([anomaly.shape[1], 1])

        for feature in range(anomaly.shape[1]):
            # find all projections without the feature j and with feature j
            index_selected_feature = cp.where(
                self._projections[:, feature] != 0)[0]
            index_not_selected_feature = cp.where(
                self._projections[:, feature] == 0)[0]
            scores_with_feature = self._instance_score(anomaly,
                                                      index_selected_feature)
            scores_without_feature = self._instance_score(
                anomaly, index_not_selected_feature)
            ranked_feature_importance[feature, 0] = self._t_test(
                scores_with_feature, scores_without_feature)

        if scaled:
            assert cp.max(ranked_feature_importance) != cp.min(
                ranked_feature_importance)
            normalized_score = (ranked_feature_importance - cp.min(
                ranked_feature_importance)) / (
                cp.max(ranked_feature_importance) - cp.min(
                    ranked_feature_importance))
            return normalized_score
        else:
            return ranked_feature_importance

    def _instance_score(self, x, projection_index):
        """
            Return scores from selected projection index.
            x (cupy.ndarray) : D x 1 feature instance.
        """
        if cp.ndim(x) < 2:
            x = x.reshape(1, -1)
        pred_scores = cp.zeros([x.shape[0], len(projection_index)])
        for i in projection_index:
            projected_data = self._projections[i, :].dot(x.T)
            inds = cp.searchsorted(self._limits[i, :self._n_bins - 1],
                                   projected_data, side='left')
            pred_scores[:, i] = -self._weights[i] * cp.log(
                self._histograms[i, inds])
        return pred_scores

    def _t_test(self, with_sample, without_sample):
        """
        compute one-tailed two-sample t-test with a test statistics according to
            t_j: \frac{\mu_j - \bar{\mu_j}}{\sqrt{\frac{s^2_j}{\norm{I_j}} +
            \frac{\bar{s^2_j}}{\norm{\bar{I_j}}}}}
        """
        return (cp.mean(with_sample) - cp.mean(without_sample)) /\
            cp.sqrt(cp.var(with_sample)**2 / len(with_sample) +
                    cp.var(without_sample)**2 / len(without_sample))