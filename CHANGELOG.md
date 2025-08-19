# Changelog

## [1.12.3](https://github.com/bzkf/fhir-to-lakehouse/compare/v1.12.2...v1.12.3) (2025-08-19)


### Bug Fixes

* correct in-batch message ordering and use a window function to get the latest message per id/request_url ([#90](https://github.com/bzkf/fhir-to-lakehouse/issues/90)) ([4a4d0b7](https://github.com/bzkf/fhir-to-lakehouse/commit/4a4d0b7dd3631514b2677cb5cbf2d1e48cf9f63a))
* **deps:** update all non-major dependencies ([#80](https://github.com/bzkf/fhir-to-lakehouse/issues/80)) ([ec7cb38](https://github.com/bzkf/fhir-to-lakehouse/commit/ec7cb3856bd059f09fb65a22adb96955624916d0))
* **deps:** update dependency typed-settings to v25 ([#84](https://github.com/bzkf/fhir-to-lakehouse/issues/84)) ([4f5bd93](https://github.com/bzkf/fhir-to-lakehouse/commit/4f5bd9322b2826b757da8f1012e026892aae4cf2))


### Miscellaneous Chores

* **deps:** update github-actions ([#79](https://github.com/bzkf/fhir-to-lakehouse/issues/79)) ([4a35930](https://github.com/bzkf/fhir-to-lakehouse/commit/4a35930f36a9da44d331848ff1606a3adca4279d))

## [1.12.2](https://github.com/bzkf/fhir-to-lakehouse/compare/v1.12.1...v1.12.2) (2025-08-03)


### Miscellaneous Chores

* dont update version in pyproject ([#83](https://github.com/bzkf/fhir-to-lakehouse/issues/83)) ([2b143c6](https://github.com/bzkf/fhir-to-lakehouse/commit/2b143c6582e65ec8c1021d6120427979f3b02ac0))


### Build

* switch to uv ([#81](https://github.com/bzkf/fhir-to-lakehouse/issues/81)) ([3ba36dc](https://github.com/bzkf/fhir-to-lakehouse/commit/3ba36dc2a6988eb5cbe9784a978ed64a10c87af4))

## [1.12.1](https://github.com/bzkf/fhir-to-lakehouse/compare/v1.12.0...v1.12.1) (2025-07-23)


### Miscellaneous Chores

* **deps:** update all non-major dependencies ([#72](https://github.com/bzkf/fhir-to-lakehouse/issues/72)) ([9358c0f](https://github.com/bzkf/fhir-to-lakehouse/commit/9358c0f89d7e66fc66c59116eae365454df43385))
* **deps:** update github-actions ([#71](https://github.com/bzkf/fhir-to-lakehouse/issues/71)) ([c1cc021](https://github.com/bzkf/fhir-to-lakehouse/commit/c1cc0214a38dc08752891f2d40f1c6de9658b623))
* **deps:** update github-actions ([#77](https://github.com/bzkf/fhir-to-lakehouse/issues/77)) ([a0ac745](https://github.com/bzkf/fhir-to-lakehouse/commit/a0ac745461fb1f60351aef84e2995360ba88c216))
* **deps:** update github-actions ([#78](https://github.com/bzkf/fhir-to-lakehouse/issues/78)) ([7bf4460](https://github.com/bzkf/fhir-to-lakehouse/commit/7bf4460467a1a68c91fea5962698eff59a2a6671))
* **deps:** update minio docker tag to v17 ([#74](https://github.com/bzkf/fhir-to-lakehouse/issues/74)) ([011e881](https://github.com/bzkf/fhir-to-lakehouse/commit/011e8819952ee549b9f26225482d478bbc5790e6))
* **renovate:** ignore py4j ([#76](https://github.com/bzkf/fhir-to-lakehouse/issues/76)) ([c07de7d](https://github.com/bzkf/fhir-to-lakehouse/commit/c07de7d33121cf31a7c341508e4b4c8b58d2364f))

## [1.12.0](https://github.com/bzkf/fhir-to-lakehouse/compare/v1.11.5...v1.12.0) (2025-06-29)


### Features

* integrated lakehousekeeper cli ([#69](https://github.com/bzkf/fhir-to-lakehouse/issues/69)) ([c07bec4](https://github.com/bzkf/fhir-to-lakehouse/commit/c07bec4195c075644cb71bf82061ec41da642942))

## [1.11.5](https://github.com/bzkf/fhir-to-lakehouse/compare/v1.11.4...v1.11.5) (2025-06-28)


### Miscellaneous Chores

* **deps:** update all non-major dependencies ([#65](https://github.com/bzkf/fhir-to-lakehouse/issues/65)) ([e311524](https://github.com/bzkf/fhir-to-lakehouse/commit/e311524f4cc88cdcccea7cb22a46d46648d6c535))
* **deps:** update dependency urllib3 to v2.5.0 [security] ([#64](https://github.com/bzkf/fhir-to-lakehouse/issues/64)) ([6d3175f](https://github.com/bzkf/fhir-to-lakehouse/commit/6d3175f09b7619b5a75e5c781f8ceb550b9b597c))
* **deps:** update docker.io/trinodb/trino docker tag to v476 ([#67](https://github.com/bzkf/fhir-to-lakehouse/issues/67)) ([05650a4](https://github.com/bzkf/fhir-to-lakehouse/commit/05650a4d315ddd2ad81d1e29e2c6bc919ff29788))
* **deps:** update github-actions ([#66](https://github.com/bzkf/fhir-to-lakehouse/issues/66)) ([34ffa61](https://github.com/bzkf/fhir-to-lakehouse/commit/34ffa61bff6813e5fb1665df84e3e678b7a08713))

## [1.11.4](https://github.com/bzkf/fhir-to-lakehouse/compare/v1.11.3...v1.11.4) (2025-06-09)


### Miscellaneous Chores

* **deps:** update all non-major dependencies ([#59](https://github.com/bzkf/fhir-to-lakehouse/issues/59)) ([02c7db1](https://github.com/bzkf/fhir-to-lakehouse/commit/02c7db19f9d3d6df27cf3c3dcd78b3424a1dc08e))
* **deps:** update docker.io/bitnami/minio:2025.5.24 docker digest to 451fe68 ([#58](https://github.com/bzkf/fhir-to-lakehouse/issues/58)) ([b7293af](https://github.com/bzkf/fhir-to-lakehouse/commit/b7293af88ac83453a2955d7080e87771a9fd2edc))
* **deps:** update github-actions ([#60](https://github.com/bzkf/fhir-to-lakehouse/issues/60)) ([d19bef3](https://github.com/bzkf/fhir-to-lakehouse/commit/d19bef3c642601f5bdf30a95e9ae846e9555902f))

## [1.11.3](https://github.com/bzkf/fhir-to-lakehouse/compare/v1.11.2...v1.11.3) (2025-06-03)


### Bug Fixes

* addressed scorecard lints and switched to releas-please ([#56](https://github.com/bzkf/fhir-to-lakehouse/issues/56)) ([ee9e4de](https://github.com/bzkf/fhir-to-lakehouse/commit/ee9e4de7c6a7ee5c3f5c57dd93ddc71b2d0a3e83))


### Miscellaneous Chores

* **deps:** update docker.io/bitnami/minio:2025.5.24 docker digest to 5273c39 ([#48](https://github.com/bzkf/fhir-to-lakehouse/issues/48)) ([8777473](https://github.com/bzkf/fhir-to-lakehouse/commit/8777473ff1c72c5f70e34a902cb4aa572407c6af))
