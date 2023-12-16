# Changelog

All notable changes to this project will be documented in this file.

## [0.1.1] - 2023-12-16

### Bug Fixes

- Fix pthread invalid argument issue. It is causd by qdrant is not terminated properly
 ([95c5774](95c5774895340672003a80909b5f0073dd1e21ad) - 2023-12-15 by Tyr Chen)
- Fix CI
 ([ceef0f0](ceef0f0c2f8004937bc01335ca20a1dc9bf7f408) - 2023-12-16 by Tyr Chen)
- Fix CI and prepare for release
 ([ad53b23](ad53b23a3a718751530c578b3304d1f3b2f94f8e) - 2023-12-16 by Tyr Chen)

### Other

- Init the project. Provide very basic functionalities.
 ([0dd9de8](0dd9de825c0062323374de78b509b79fc81c315c) - 2023-12-15 by Tyr Chen)
- Prepare for git repo
 ([765c8aa](765c8aabe4c86055ebd1ae20671f963807ec679e) - 2023-12-15 by Tyr Chen)
- Use thiserror for better error handling
 ([efd6559](efd6559d793dfc8c4f7cf9193ac837ff61eb2f6e) - 2023-12-15 by Tyr Chen)
- Add the rest of the APIs and provide indexer/searcher examples
 ([9ab234d](9ab234dd4d25a57ed3f9142065c14848212206fb) - 2023-12-16 by Tyr Chen)
- Remove qdrant/qdrant
 ([156d360](156d360d48261f69af0fb3ff4a6310f748511f8e) - 2023-12-16 by Tyr Chen)
- Remove actix-web-validator from lib/collection in tyrchen/qdrant. It brings in lots of actix dependencies which are not necessary
 ([9bd6877](9bd68771f1f82aa805b9bafbb3ad3e38ac0a9097) - 2023-12-16 by Tyr Chen)
- Update readme
 ([6e8aa8f](6e8aa8f3c0217ced2eb927bc9ffc18db89047cc8) - 2023-12-16 by Tyr Chen)
- Add get points API and update CI & readme
 ([963d2e2](963d2e2b118346cf80a304c1c803172bf6a5cc45) - 2023-12-16 by Tyr Chen)
- Small doc update
 ([cfe2e1b](cfe2e1bcb744267f2dc98e6368b791db20c7dad4) - 2023-12-16 by Tyr Chen)

<!-- generated by git-cliff -->