// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Demikernel.Interop;

/// <summary>
/// Represents a Demikernel socket instance
/// </summary>
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 8)]
public readonly struct QueueToken : IEquatable<QueueToken>
{
    [FieldOffset(0)]
    internal readonly System.Int64 Qt;

    /// <summary>Compare two values for equality</summary>
    public bool Equals(QueueToken other) => other.Qt == Qt;

    /// <inheritdoc/>
    public override bool Equals([NotNullWhen(true)] object? obj)
        => obj is QueueToken other && other.Qt == Qt;

    /// <inheritdoc/>
    public override int GetHashCode() => Qt.GetHashCode();

    /// <inheritdoc/>
    public override string ToString() => $"Queue-token {Qt}";

    /// <summary>Compare two values for equality</summary>
    public static bool operator ==(QueueToken x, QueueToken y) => x.Qt == y.Qt;
    /// <summary>Compare two values for equality</summary>
    public static bool operator !=(QueueToken x, QueueToken y) => x.Qt != y.Qt;
}
