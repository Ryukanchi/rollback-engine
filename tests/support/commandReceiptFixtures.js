function commandReceiptMetadata({ domainEffect = "none" } = {}) {
  return {
    contractVersion: 1,
    domainEffect,
    stateAnchor: null,
  };
}

module.exports = { commandReceiptMetadata };
