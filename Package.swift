import PackageDescription

let package = Package(
    name: "Swirl",
    targets: [
        Target(
            name: "Swirl"
        ),
        Target(
            name: "SwirlSQLite",
            dependencies: [.Target(name: "Swirl")]
        ),
        Target(
            name: "Demo",
            dependencies: [.Target(name: "Swirl"), .Target(name: "SwirlSQLite")]
        )
    ],
    dependencies: [
        .Package(url: "https://github.com/reactive-swift/RDBC.git", majorVersion: 0, minor: 2),
        .Package(url: "https://github.com/crossroadlabs/RDBCSQLite.git", majorVersion: 0, minor: 2),
    ]
)
