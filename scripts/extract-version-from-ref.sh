if [[ "${GITHUB_REF:-}" != "refs/tags/"* ]]; then
    echo "The GITHUB_REF variable does not contain a tag reference."
    exit 0
fi

tag="${GITHUB_REF#refs/tags/}"

echo "Parsing version: ${tag}"

major=$(semver get major "${tag}")
minor=$(semver get minor "${tag}")
patch=$(semver get patch "${tag}")
prerel=$(semver get prerel "${tag}")

version="${major}.${minor}.${patch}"
if [[ -n "${prerel}" ]]; then
    version="${version}-${prerel}"
fi

echo "version=${version}"
echo "major=${major}"
echo "minor=${minor}"
echo "patch=${patch}"
echo "prerel=${prerel}"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "Writing output to ${GITHUB_OUTPUT}"
    {
        echo "version=${version}"
        echo "major=${major}"
        echo "minor=${minor}"
        echo "patch=${patch}"
        echo "prerel=${prerel}"
    } >> "$GITHUB_OUTPUT"
fi
